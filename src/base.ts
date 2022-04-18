import {resolve} from "path";
import {Stream} from "stream";
import split from "split";
import {
  createReadStream,
  createWriteStream,
  WriteStream,
  existsSync,
  unlinkSync,
} from "fs";

export const getSource = (pathBase: string) => {
  if (!pathBase) throw new Error("Source file not props");
  pathBase = pathBase.replace("./", "/");
  console.log("load file...");
  const fileStream = createReadStream(resolve(process.cwd() + "/" + pathBase));
  return fileStream.pipe(split()); //разбили на строки
};

interface StreamContext {
  stream: WriteStream;
  counter: number;
  chunk: number;
}

const createStream = (name: string) => {
  const path = resolve(process.cwd() + "/" + name);
  if (existsSync(path)) unlinkSync(path);
  return createWriteStream(path, {
    flags: "a", //a - добавить строки в конец файла
  });
};

const streams: {[k: string]: StreamContext} = {};
const getCtxS = (name: string) => {
  if (!streams[name]) {
    return (streams[name] = {
      stream: createStream(name),
      counter: 0,
      chunk: 0,
    });
  }
  return streams[name];
};

const writeFile = (name: string, line: string, maxSize?: number) => {
  const ctx = getCtxS(name);
  ctx.stream.write(line);
  ctx.counter += line.length;
  if (maxSize) {
    if (ctx.counter > maxSize) {
      ctx.chunk++;
      console.log(`\n${name} - chank: ${ctx.chunk}`);
      ctx.counter = 0;
      ctx.stream.destroy();
      ctx.stream = createStream(`${ctx.chunk}_${name}`);
      ctx.counter = 0;
    }
  }
};

export const SPLIT_SYMBOL = "ﺦ"; //этого символа в базе точно не будет

export const saveBase = (st: Stream, maxSize?: number) => {
  st.on("data", data => {
    const [interval, line] = data.toString().split(SPLIT_SYMBOL);
    if (line) {
      writeFile(`${interval}_out.scv`, line + "\n", maxSize);
    } else {
      writeFile(`out.scv`, interval + "\n", maxSize);
    }
  });

  st.on("end", () => {
    Object.values(streams).map(({stream}) => stream.destroy());
  });
};
