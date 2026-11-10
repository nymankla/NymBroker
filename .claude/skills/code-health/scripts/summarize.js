// usage: node summarize.js <report.json>  — report written by CodeHealthTests (NYMBROKER_CODE_HEALTH_REPORT); prints production/test/sample stats, hotspots, largest types/files, per-area averages, duplicates
const d = JSON.parse(require('fs').readFileSync(process.argv[2], 'utf8'));
const prod = m => !['NymBroker.Tests','samples'].includes(m.area);
const pct = (a,p) => { const s=[...a].sort((x,y)=>x-y); return s[Math.min(s.length-1,Math.floor(p*s.length))]; };
for (const [label, f] of [['PRODUCTION', prod], ['TESTS', m=>m.area==='NymBroker.Tests'], ['SAMPLES', m=>m.area==='samples']]) {
  const ms = d.methods.filter(f), fs = d.files.filter(f);
  const L = ms.map(m=>m.lines), C = ms.map(m=>m.cc), G = ms.map(m=>m.cognitive);
  console.log(`\n== ${label}: files=${fs.length} codeLines=${fs.reduce((a,x)=>a+x.code,0)} methods=${ms.length}`);
  console.log(` lines  p50=${pct(L,.5)} p90=${pct(L,.9)} p99=${pct(L,.99)} max=${Math.max(...L)} | >50: ${L.filter(x=>x>50).length} >100: ${L.filter(x=>x>100).length}`);
  console.log(` CC     p50=${pct(C,.5)} p90=${pct(C,.9)} p99=${pct(C,.99)} max=${Math.max(...C)} | >10: ${C.filter(x=>x>10).length} >20: ${C.filter(x=>x>20).length}`);
  console.log(` cognit p50=${pct(G,.5)} p90=${pct(G,.9)} p99=${pct(G,.99)} max=${Math.max(...G)} | >15: ${G.filter(x=>x>15).length} >25: ${G.filter(x=>x>25).length}`);
  console.log(` nesting>=4: ${ms.filter(m=>m.nesting>=4).length}  params>=6: ${ms.filter(m=>m.parameters>=6).length}`);
}
const P = d.methods.filter(prod);
const row = m => `${String(m.lines).padStart(4)} L  cc ${String(m.cc).padStart(3)}  cog ${String(m.cognitive).padStart(3)}  nest ${m.nesting}  p ${m.parameters}  ${m.type}.${m.name}  ${m.file}:${m.line}`;
console.log('\n== PROD: top 25 by cognitive'); P.sort((a,b)=>b.cognitive-a.cognitive||b.cc-a.cc).slice(0,25).forEach(m=>console.log(row(m)));
console.log('\n== PROD: top 15 by length'); P.sort((a,b)=>b.lines-a.lines).slice(0,15).forEach(m=>console.log(row(m)));
console.log('\n== PROD: params >= 6'); P.filter(m=>m.parameters>=6).sort((a,b)=>b.parameters-a.parameters).slice(0,15).forEach(m=>console.log(row(m)));
const T = d.types.filter(prod);
console.log('\n== PROD: largest types'); T.sort((a,b)=>b.lines-a.lines).slice(0,15).forEach(t=>console.log(`${String(t.lines).padStart(5)} L  members ${String(t.members).padStart(3)}  fields ${String(t.fields).padStart(2)}  ctorP ${t.ctorParams}  ${t.kind} ${t.name}  ${t.file}`));
console.log('\n== PROD: largest files'); d.files.filter(prod).sort((a,b)=>b.code-a.code).slice(0,12).forEach(f=>console.log(`${String(f.code).padStart(5)}  ${f.file}`));
console.log('\n== PROD: per-area'); const by={}; for(const m of P){(by[m.area]??={n:0,l:0,cog:0,hot:0}); by[m.area].n++; by[m.area].l+=m.lines; by[m.area].cog+=m.cognitive; if(m.cognitive>15)by[m.area].hot++;}
Object.entries(by).sort((a,b)=>b[1].l-a[1].l).forEach(([k,v])=>console.log(`${k.padEnd(36)} methods ${String(v.n).padStart(4)}  avgLen ${(v.l/v.n).toFixed(1).padStart(5)}  avgCog ${(v.cog/v.n).toFixed(2).padStart(5)}  cog>15: ${v.hot}`));
console.log('\n== Duplicate 10-line windows'); d.duplicates.forEach(g=>console.log(`${g.windows} windows: ${g.firstLines.join('  ,  ')}`));
const TT = d.methods.filter(m=>m.area==='NymBroker.Tests');
console.log('\n== TESTS: top 8 by length'); TT.sort((a,b)=>b.lines-a.lines).slice(0,8).forEach(m=>console.log(row(m)));
console.log('\n== TEST files largest'); d.files.filter(f=>f.area==='NymBroker.Tests').sort((a,b)=>b.code-a.code).slice(0,8).forEach(f=>console.log(`${String(f.code).padStart(5)}  ${f.file}`));
