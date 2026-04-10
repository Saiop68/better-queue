const fs = require('fs');
const path = require('path');
const scriptsDir = path.join(__dirname, 'scripts');
const files = fs.readdirSync(scriptsDir).filter(f => f.endsWith('.lua'));

let output = '/* eslint-disable */\n// Auto-generated file. Do not edit directly.\nexport const LUA = {\n';

for (const file of files) {
  const content = fs.readFileSync(path.join(scriptsDir, file), 'utf8');
  const key = file.replace('.lua', '').replace(/_([a-z])/g, g => g[1].toUpperCase());
  
  // Escape backticks and backslashes for JS strings
  const escapedContent = content
    .replace(/\\/g, '\\\\')
    .replace(/`/g, '\\`');
    
  output += `  ${key}: \`${escapedContent}\`,\n`;
}

output += '};\n';
fs.writeFileSync(path.join(__dirname, 'src', 'lua-scripts.ts'), output);
console.log('Successfully generated src/lua-scripts.ts');
