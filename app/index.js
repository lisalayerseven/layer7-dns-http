import { exec } from "child_process";

const target = process.env.TEST_DOMAIN || "josef.org";
console.log(`Running dig against Unbound for ${target}...`);

exec(`dig @unbound ${target} +short`, (err, stdout, stderr) => {
  if (err) {
    console.error("Error running dig:", err);
    process.exit(1);
  }
  console.log("=== dig result ===");
  console.log(stdout || stderr || "No output.");
  console.log("==================");
});
