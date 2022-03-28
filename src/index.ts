// выборка по координатам
// Фильтрация по дублям по номеру
// рзбивние по диапазону

import {Command} from "commander";
import {getSource, saveBase} from "./base";
import {IsPointToSquare, KmToDec, Point} from "./calc";
import {getSettings} from "./settings";

const cliApp = new Command();

interface Options {
  gpsPoint?: string;
  radius?: number;
  interval?: string;
  deduplication?: boolean;
}

const processPrint = (prefix = "", i: number, src: unknown[], step = 1000) => {
  if (i % step === 0)
    process.stdout.write(`\r${prefix}: ${i} is ${src.length}`);
};

cliApp
  .description("parser csv geo")
  .argument("[string]", "source csv file")
  .option("-gp, --gpsPoint [string]", "gps point")
  .option("-r, --radius [number]", "radius search gps(km)")
  .option("-d, --deduplication ", "deduplication")
  .option("-i, --interval [string]", "1,22,33")
  .action(
    async (
      _,
      {gpsPoint, radius = 10, deduplication, interval}: Options,
      cmd: Command,
    ) => {
      const settings = getSettings();
      let src = getSource(cmd.args[0]);
      console.log("base: ", cmd.args[0], src.length);
      if (gpsPoint) {
        const [x, y] = gpsPoint.split(",").map(Number);
        const radiusDec = KmToDec(radius);
        const check = (p: Point) => IsPointToSquare(p, {x, y}, radiusDec);
        src = src.filter((line, i) => {
          processPrint("geo", i, src, 5000);
          return check({
            x: Number(line[settings.latitude]),
            y: Number(line[settings.longitude]),
          });
        });
        console.log("\nfilter geo: ", src.length);
      }

      if (deduplication) {
        const checkList: string[] = [];
        src = src.filter((line, i) => {
          processPrint("dedup", i, src);
          const key = line[settings.deduplication];
          if (checkList.includes(key)) return false;
          checkList.push(key);
          return true;
        });
        console.log("\nfilter deduplication: ", src.length);
      }
      if (interval) {
        console.log(interval);
        let intList = interval.split(",").map(Number);
        intList.unshift(0);
        intList.push(Infinity);
        const map: {[k: string]: string[][]} = {};
        intList = intList.sort();
        let i = 0;
        for (const line of src) {
          i++;
          processPrint("interval:", i, src);
          for (const int of intList) {
            if (int >= Number(line[settings.interval])) {
              if (!map[int]) map[int] = [];
              map[int].push(line);
              break;
            }
          }
        }
        console.log("\n"); //для перекрытия processPrint
        for (let index = 1; index < intList.length; index++) {
          const current = intList[index];
          const back = intList[index - 1];
          const list = map[current];
          const intervalH = `${back}-${current}`;
          console.log(`${intervalH}: `, list.length);
          saveBase(list, `out_${intervalH}.csv`);
        }
      } else {
        saveBase(src, "out.csv");
      }
    },
  );
cliApp.parse();
