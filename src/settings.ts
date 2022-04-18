import {resolve} from "path";
import {existsSync, readFileSync} from "fs";

//структура настроек
//описывает в каком столбце что искать
//номер столбца указываем
export interface Settings {
  latitude: number; //широта
  longitude: number; //долгота
  deduplication: number; //по этому столбцу ищем дубли
  interval: number[]; //по этому столбцу разбиваем на интервалы
  outColumn?: number[]; //какие столбцы выводим в итоговый файл
  phoneColumn?: number[]; //форматируем под телефонный номер
}

const SETTINGS_NAME = resolve(process.cwd() + "/settings.json");

export const getSettings = () => {
  if (!existsSync(SETTINGS_NAME)) throw new Error("Settings file not found!");
  const settings: Settings = JSON.parse(readFileSync(SETTINGS_NAME).toString());
  if (settings.deduplication === undefined)
    throw new Error("Deduplication props for settings file not found!");
  if (!settings.longitude === undefined)
    throw new Error("Longitude props for settings file not found!");
  if (!settings.latitude === undefined)
    throw new Error("Latitude props for settings file not found!");
  if (!settings.interval === undefined)
    throw new Error("Interval props for settings file not found!");
  return settings;
};
