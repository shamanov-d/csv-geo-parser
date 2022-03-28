import {resolve} from "path";
import {readFileSync, writeFileSync} from "fs";

export const getSource = (pathBase: string) => {
  if (!pathBase) throw new Error("Source file not props");
  pathBase = pathBase.replace("./", "/");
  const str = readFileSync(resolve(process.cwd() + "/" + pathBase));
  return str
    .toString()
    .split("\n")
    .map(line => line.split(","));
};

export const saveBase = (line: string[][], name: string) => {
  writeFileSync(
    resolve(process.cwd() + "/" + name),
    line.map(line => line.join(",")).join("\n"),
  );
};
