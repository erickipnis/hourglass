require('babel-core/register');

export default function (redisURL) {
  const redisPath = redisURL || 'redis://localhost:6379';
  const hourglass =

  return hourglass;
}
