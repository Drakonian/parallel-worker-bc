Compile the AL Business Central project in two steps:

**Step 1 — Compile app with all analyzers:**

```
ANALYZER_DIR="C:\Users\Drakonian\.dotnet\tools\.store\microsoft.dynamics.businesscentral.development.tools\16.2.28.57946\microsoft.dynamics.businesscentral.development.tools\16.2.28.57946\tools\net8.0\any"
cd "$(git rev-parse --show-toplevel)/app" && MSYS_NO_PATHCONV=1 al compile /project:"./" /packagecachepath:".\.alpackages" /analyzer:"$ANALYZER_DIR\Microsoft.Dynamics.Nav.CodeCop.dll" /analyzer:"$ANALYZER_DIR\Microsoft.Dynamics.Nav.PerTenantExtensionCop.dll" /analyzer:"$ANALYZER_DIR\Microsoft.Dynamics.Nav.AppSourceCop.dll" /analyzer:"$ANALYZER_DIR\Microsoft.Dynamics.Nav.UICop.dll" /ruleset:"./.codeAnalysis/al.ruleset.json"
```

**Step 2 — Copy updated app to test packages and compile test:**

```
ROOT="$(git rev-parse --show-toplevel)"
cp "$ROOT/app/Volodymyr Dvernytskyi_Parallel Worker_1.0.0.0.app" "$ROOT/test/.alpackages/" && cd "$ROOT/test" && MSYS_NO_PATHCONV=1 al compile /project:"./" /packagecachepath:".\.alpackages"
```

Report the compilation results for both steps. If there are errors or warnings, analyze them and suggest fixes.
