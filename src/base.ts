import {resolve} from "path";
import {Stream} from "stream";
import split from "split";
import {createReadStream, createWriteStream, WriteStream} from "fs";

export const getSource = (pathBase: string) => {
  if (!pathBase) throw new Error("Source file not props");
  pathBase = pathBase.replace("./", "/");
  console.log("load file...");
  const fileStream = createReadStream(resolve(process.cwd() + "/" + pathBase));
  return fileStream.pipe(split()); //разбили на строки
};

const streams: {[k: string]: WriteStream} = {};
const getSt = (name: string) => {
  if (!streams[name])
    return (streams[name] = createWriteStream(
      resolve(process.cwd() + "/" + name),
      {
        flags: "a", //a - добавить строки в конец файла
      },
    ));
  return streams[name];
};

export const SPLIT_SYMBOL = "ﺦ"; //этого символа в базе точно не будет

export const saveBase = (st: Stream) => {
  st.on("data", data => {
    const [interval, line] = data.toString().split(SPLIT_SYMBOL);
    if (line) {
      const st = getSt(`${interval}_out.scv`);
      st.write(line);
    } else {
      const st = getSt("out.scv");
      st.write(interval + "\n"); //если нет потока то пишем в основной файл
    }
  });

  st.on("end", () => {
    Object.values(streams).map(st => st.destroy());
  });
};
