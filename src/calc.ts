export interface Point {
  x: number;
  y: number;
}

// проверям находится ли точка в окружности
//! все кодинаты должны быть в одной системе
export const IsPointToCircle = (point: Point, circle: Point, r: number) => {
  return (point.x - circle.x) ** 2 + (point.y - circle.y) ** 2 === r ** 2;
};

// проверяем на вхождение в квадрат
export const IsPointToSquare = (point: Point, circle: Point, r: number) => {
  const xStart = circle.x - r / 2;
  const xEnd = circle.x + r / 2;
  const yStart = circle.y - r / 2;
  const yEnd = circle.y + r / 2;

  if (xStart < point.x && point.x < xEnd)
    if (yStart < point.y && point.y < yEnd) return true;

  return false;
};

const ec = 40075.696 / 360; // 1 градус на экваторе
// перевели километры в десятичные градусы
export const KmToDec = (r: number) => {
  return Math.acos(r / ec);
};
