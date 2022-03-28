// выборка по координатам
// Фильтрация по дублям по номеру
// рзбивние по диапазону
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
    this.interval = interval.sort();
  }
  _transform(
    line: Buffer,
    encoding: BufferEncoding,
    callback: TransformCallback,
  ) {
    const lineArr = line.toString().split(",");
    for (let index = 1; index < this.interval.length; index++) {
      const current = this.interval[index];
      const back = this.interval[index - 1];
      if (current >= Number(lineArr[this.settings.interval])) {
        callback(null, `${back}-${current}${SPLIT_SYMBOL}${line}`);
        break;
      }
    }
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
  .action(
    (
      _,
      {gpsPoint, radius = 10, deduplication, interval}: Options,
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
        if (deduplication)
          srcStreamLine = srcStreamLine.pipe(new Deduplication(settings));
        if (interval)
          srcStreamLine = srcStreamLine.pipe(
            new SplitInterval(settings, interval.split(",").map(Number)),
          );

        saveBase(srcStreamLine);
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
