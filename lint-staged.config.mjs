export default {
  "spec/**/*": () => "node scripts/check-spec.mjs",
  "js/**/*.{ts,mjs,json,md}": () => ["make check-js"],
  "go/**/*.go": () => ["make check-go"],
};
