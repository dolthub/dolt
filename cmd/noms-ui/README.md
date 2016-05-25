# noms-ui

Serves noms browser UIs, from its own web and noms database server.

```
noms ui [-port PORT] directory [args...]
```

* `-port` serves on a custom port. By default `noms-ui` chooses a random port.
* `directory` specifies the directory of the browser UI. Browser URLs are relative to this directory, with `index.html` by default.
* `args` are a list of arguments to pass to the UI of the form `arg1=val`, `arg2=val2`, etc. `ldb:` values are automatically translated into paths to an HTTP noms database server.
