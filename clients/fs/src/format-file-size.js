// @flow

const units = ['', 'K', 'M', 'G', 'T', 'P'];

export default function formatFileSize(n: number): string {
  if (n < 1) {
    return n + 'B';
  }

  const exp = Math.min(Math.floor(Math.log(n) / Math.log(1024)), units.length - 1);
  n = Number((n / Math.pow(1024, exp)).toFixed(1));

  return n + units[exp] + 'B';
}
