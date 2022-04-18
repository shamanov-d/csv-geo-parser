import {Transform, Stream, TransformCallback} from "stream";
import {Command} from "commander";
import {getSource, saveBase, SPLIT_SYMBOL} from "./base";
import {IsPointToSquare, KmToDec, Point} from "./calc";
import {getSettings, Settings} from "./settings";

const cliApp = new Command();

interface Options {
  gpsPoint?: string;
  radius?: number;
  interval?: string;
  deduplication?: boolean;
  filterOutput?: boolean;
  phoneFormat?: boolean;
  maxFileSize?: number;
}

// фильтруем поток по координатам
class GeoFilter extends Transform {
  private check!: (p: Point) => boolean;
  constructor(private settings: Settings, radius: number, gpsPoint: string) {
    super();
    console.log(`start point ${gpsPoint}, radius ${radius} km`);
    const [x, y] = gpsPoint.split(",").map(Number);
    const radiusDec = KmToDec(radius);
    this.check = (p: Point) => IsPointToSquare(p, {x, y}, radiusDec);
  }
  _transform(
    line: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    const lineArr = line.toString().split(",");
    if (
      this.check({
        x: Number(lineArr[this.settings.latitude]),
        y: Number(lineArr[this.settings.longitude]),
      })
    )
      return callback(null, line);
    callback();
  }
}

// дедубликация
class Deduplication extends Transform {
  private doubleKey = new Set<string>();
  constructor(private settings: Settings) {
    super();
  }
  _transform(
    line: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    const lineArr = line.toString().split(",");
    const key = lineArr[this.settings.deduplication];
    if (this.doubleKey.has(key)) return callback();
    this.doubleKey.add(key);
    callback(null, line);
  }
}

// разбиваем по интервалам
class SplitInterval extends Transform {
  constructor(private settings: Settings, private interval: number[]) {
    super();
    interval.unshift(0);
    interval.push(Infinity);
    this.interval = interval.sort((a, b) => a - b);
  }
  _transform(
    line: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    const lineArr = line.toString().split(",");
    // выбирает столбец ответсвенный за сортировку по цене, если их может быть несколько
    const num = this.settings.interval.reduce<number>((acc, val) => {
      if (String(lineArr[val]).includes(".")) return acc;
      const n = Number(lineArr[val]);
      if (!isNaN(n)) return n;
      return acc;
    }, NaN);
    if (!isNaN(num))
      for (let index = 1; index < this.interval.length; index++) {
        const current = this.interval[index];
        const back = this.interval[index - 1];
        if (current >= num) {
          callback(null, `${back}-${current}${SPLIT_SYMBOL}${line}`);
          return;
        }
      }
    console.log({lineArr, num});
    // throw new Error("Interval column is NaN");
    return callback();
  }
}

class OutColumnFilter extends Transform {
  constructor(private column: number[]) {
    super();
    this.column = column.sort();
  }
  _transform(
    line: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    let lineArr = line.toString().split(",");
    lineArr = lineArr.filter((_, i) => this.column.includes(i));
    callback(null, lineArr.join(","));
  }
}

class PhoneFormatter extends Transform {
  constructor(private column: number[]) {
    super();
    this.column = column.sort();
  }
  _transform(
    line: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    const lineArr = line.toString().split(",");
    for (const col of this.column) {
      lineArr[col] = lineArr[col]
        .replace("+7", "7")
        .replace("-", "-")
        .replace("(", "")
        .replace(")", "");
    }
    callback(null, lineArr.join(","));
  }
}

class Print extends Transform {
  private count = 0;
  _transform(
    line: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    this.count++;
    if (this.count % 1000 === 0)
      process.stdout.write(`\rprocessing element:${this.count}`);
    callback(null, line.toString());
  }
}

cliApp
  .description("parser csv geo")
  .argument("[string]", "source csv file")
  .option("-gp, --gpsPoint [string]", "gps point")
  .option("-r, --radius [number]", "radius search gps(km)")
  .option("-d, --deduplication ", "deduplication")
  .option("-i, --interval [string]", "1,22,33")
  .option("-fo, --filterOutput", "filter output csv")
  .option("-pf, --phoneFormat", "phone formatter")
  .option("-mfs, --maxFileSize [number]", "phone formatter mb")
  .action(
    (
      _,
      {
        gpsPoint,
        radius = 10,
        deduplication,
        interval,
        filterOutput,
        phoneFormat,
        maxFileSize,
      }: Options,
      cmd: Command,
    ) => {
      return new Promise(res => {
        const startTime = Date.now();
        const settings = getSettings();
        let srcStreamLine: Stream = getSource(cmd.args[0]);

        srcStreamLine = srcStreamLine.pipe(new Print());

        if (gpsPoint)
          srcStreamLine = srcStreamLine.pipe(
            new GeoFilter(settings, radius, gpsPoint),
          );

        if (phoneFormat) {
          if (!settings.phoneColumn)
            throw new Error("phoneColumn props for settings file not found!");
          srcStreamLine = srcStreamLine.pipe(
            new PhoneFormatter(settings.phoneColumn),
          );
        }

        if (deduplication)
          srcStreamLine = srcStreamLine.pipe(new Deduplication(settings));
        if (interval)
          srcStreamLine = srcStreamLine.pipe(
            new SplitInterval(settings, interval.split(",").map(Number)),
          );

        if (filterOutput) {
          if (!settings.outColumn)
            throw new Error("OutColumn props for settings file not found!");
          srcStreamLine = srcStreamLine.pipe(
            new OutColumnFilter(settings.outColumn),
          );
        }

        if (maxFileSize) maxFileSize = maxFileSize * 1024 * 1024;
        saveBase(srcStreamLine, maxFileSize);
        srcStreamLine.on("end", () => {
          console.log(
            `\nfilter completed ${(Date.now() - startTime) / 1000} s.`,
          );
          res();
        });
      });
    },
  );
cliApp.parse();
