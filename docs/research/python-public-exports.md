# How should a typed, lazily-loaded Python library declare its public names?

**Status:** research notes, 2026-10-02. Not a decision — no ADR is implied by this
file, and nothing here changes source.

---

## The answer, up front

Type checkers do not run `__init__.py`. They read it, and they only believe a name
is public if it is written down in one of a few fixed ways: an `__all__` that is a
plain list of strings, or an import spelled `from .x import y as y`. Rakaia does
neither. Its `__all__` is built at runtime from a dictionary, and the typed import
block uses plain `from .x import y`. So whether a consumer's `from rakaia import
StreamStore` passes depends on which checker they run and which version of it.
Measured against the 0.7.0 wheel: **pyright 1.1.409 rejects every name, mypy
`--strict` rejects every name, and `from rakaia import *` under mypy brings in only
`app` and `__version__`**. Pyright 1.1.410 and later accept the names, because of an
undocumented change that treats a list it cannot read as "assume normal names are
public". Our own CI already prints the warning on every run. It is a warning, not an
error, so nothing fails.

The smallest fix that works for every checker is to write `__all__` out as a
literal list and keep `_EXPORTS` only for the lazy runtime lookup. A test then
checks the two match, which is how pydantic does it. Changing the typed imports to
`y as y` also fixes named imports, but it does not fix `import *` under mypy. The
check that would have caught this before 0.7.0 is to build the wheel, install it in
a clean environment, and type-check a generated consumer file that imports every
name in `__all__`, using both pyright and `mypy --strict`. Lazy loading is worth
keeping: it is what stops importing the framework from starting a server, and the
type-checker problem comes from how the names are declared, not from loading them
lazily. PEP 810's `lazy` imports could eventually replace the hand-written
`__getattr__`, but only once Python 3.15 is the oldest version rakaia supports.

---

## 1. What rakaia does today, and why

`src/rakaia/__init__.py` has three parts:

- an `if TYPE_CHECKING:` block of plain `from .x import y` lines, plus bare
  annotations for the two computed names `app: Any` and `__version__: str`;
- `_EXPORTS: dict[str, str]`, which maps each name to the module that defines it;
  `__getattr__` (PEP 562) uses it to import a name the first time it is touched;
- `__all__ = [*sorted(_EXPORTS), "app", "__version__"]`.

`replay` is the exception. It is imported eagerly as `from .replay import replay`
because its name collides with its own submodule. That import is also a plain
import, so it has the same problem.

**Why it is lazy.** ddec8bb (#240, 2026-08-26), *"Importing the framework no
longer starts a server"*. When everything was imported eagerly, `import rakaia`
loaded all ten protocol-server modules and ran `app = create_app()` at module
scope. That cost 80 ms, against 37 ms for the framework alone, and every process
got an in-memory store it never asked for. With lazy loading, a framework-only
consumer pays about 3 ms. The tier split it protects is ADR 0002 and
`tests/test_rakaia/test_tier_boundary.py`. `django_rakaia` is lazy for a
different reason: eager imports raise `AppRegistryNotReady` during Django
startup.

**Why it is written this way.** The `[tool.ruff.lint.per-file-ignores]` comment in
`pyproject.toml` says outright that the redundant-alias form was considered and
turned down as "unreadable at this size". `test_public_api.py` checks that
`_EXPORTS`, the `TYPE_CHECKING` block and the expected set agree at runtime. It
cannot check what a type checker makes of them.

**What rakaia's own CI sees.** Running pyright 1.1.411 with the repo's config
prints `warning: Operation on "__all__" is not supported, so exported symbol list
may be incorrect (reportUnsupportedDunderAll)` at `src/rakaia/__init__.py:367` and
`src/django_rakaia/__init__.py:141`. The default severity for that rule is
`warning` in `standard` mode, so the gate passes.

---

## 2. The typing spec, PEP 484 and PEP 561

The typing spec's *Library interface (public and private symbols)* section, which
came from PEP 561, sets the rules for any `py.typed` package
([spec L364–417](https://github.com/python/typing/blob/dc0a8a924c99c6d6ab70f5dc4708e8e4017386ae/docs/spec/distributing.rst#L364-L417)):

- Names that begin with an underscore, other than dunders, are private.
- **"Imported symbols are considered private by default."** Only these import forms
  re-export a name: `import X as X`, `from Y import X as X`, and `from Y import *`.
- An `__all__` overrides the other rules, so anything listed in it is public. It has
  to be written in one of the forms the spec lists, *"these restrictions allow type
  checkers to statically determine the value of `__all__`"*:
  `__all__ = ('a', 'b')`, `__all__ = ['a', 'b']`, `__all__ += ['a', 'b']`,
  `__all__ += submodule.__all__`, `__all__.extend([...])`,
  `__all__.extend(submodule.__all__)`, `__all__.append('a')`, `__all__.remove('a')`.
  `[*sorted(_EXPORTS), ...]` is not one of them.

PEP 484 first stated the redundant-alias rule for stubs. Its later update says *"only
names imported using the form `X as X` will be exported"*. It also says that a
`from .ham import Ham` in a package's `__init__.pyi` exports the *submodule*
`ham`, not `Ham`. A stub may declare `def __getattr__(name) -> Any: ...` to mark
itself incomplete, which turns every undefined name into `Any`
([PEP 484 L1647–1680](https://github.com/python/peps/blob/e449c7e446faec3ecf953e83fbe677b771bf5d7c/peps/pep-0484.rst#L1647-L1680)).
PEP 562 points back to that rule when it adds module `__getattr__` at runtime
([PEP 562](https://peps.python.org/pep-0562/)).

`TYPE_CHECKING` is specified as *"considered `True` during type checking … but
`False` at runtime"*
([directives L112–134](https://github.com/python/typing/blob/dc0a8a924c99c6d6ab70f5dc4708e8e4017386ae/docs/spec/directives.rst#L112-L134)).
The spec does not give imports inside that block any special export status, so the
ordinary import rules apply there too.

---

## 3. Pyright

**Typed-libraries doc** ([1.1.411, L34–50](https://github.com/microsoft/pyright/blob/1.1.411/docs/typed-libraries.md#L34-L50)).
It repeats the spec's rules and adds two pyright-specific forms. `from . import A`
re-exports `A`. In an `__init__.py`, `from .A import X` makes the submodule `A`
public, *"but 'X' is still private"*. That second case is exactly rakaia's
situation.

**`reportPrivateImportUsage`** fires when code uses a name from a `py.typed` module
that the module does not export. It defaults to `error` in `basic`, `standard` and
`strict` mode
([configuration L158, L395](https://github.com/microsoft/pyright/blob/1.1.411/docs/configuration.md#L158)).
**`reportUnsupportedDunderAll`** defaults to `warning` in basic and standard mode
and `error` in strict
([L222, L371](https://github.com/microsoft/pyright/blob/1.1.411/docs/configuration.md#L222)).

**The undocumented 1.1.410 change.** In the 1.1.411 binder, when `__all__` uses an
unsupported form, pyright *"fall[s] back to name-convention heuristics so that
underscore-prefixed names are still treated as private while normally-named symbols
avoid false positives"*
([binder.ts L338–376](https://github.com/microsoft/pyright/blob/1.1.411/packages/pyright-internal/src/analyzer/binder.ts#L338-L376)).
Git blame traces it to [#11396](https://github.com/microsoft/pyright/pull/11396),
"Push pylance changes to pyright", merged 2026-04-21. The GitHub compare API shows
the commit is in 1.1.410, released 2026-05-23, and not in 1.1.409. Neither release's
notes mention it, and the typed-libraries doc still describes the strict rule.

**`--verifytypes <pkg>`** checks that every symbol in a package's public interface
has a known type, and prints a *type completeness score*. `--ignoreexternal` skips
incomplete types that come from other packages. The doc says to *"create a clean
Python environment and install your package"*
([L210–222](https://github.com/microsoft/pyright/blob/1.1.411/docs/typed-libraries.md#L210-L222)),
so yes, the package has to be installed. In my runs `--pythonpath` alone reported
`No py.typed file found`, and setting `VIRTUAL_ENV`/`PATH` to the venv worked.
**It does not report this bug as an error.** Under 1.1.409, `rakaia.StreamStore`
simply disappears from the `--outputjson` symbol list (25 top-level symbols instead
of 156 under 1.1.411), and the score actually rises, from 95% to 96.1%. You would only notice if you
compared the symbol list against the names you expected.

---

## 4. mypy and stubtest

**`--no-implicit-reexport`** (`implicit_reexport = False`): *"not re-export unless
the item is imported using from-as or is included in `__all__`"*, and *"always
treated as enabled for stub files"*
([command_line L704–725](https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/docs/source/command_line.rst#L704-L725)).
`--strict` turns it on, along with the `disallow-*` and `warn-*` flags shown in
`mypy --help` (checked on 2.4.0). The docs say the exact list *"may change over
time"* ([L823–842](https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/docs/source/command_line.rst#L823-L842)).

**How mypy reads a dynamic `__all__`.** `process__all__` only handles a list or
tuple on the right-hand side, and `add_exports` keeps only the items that are string
literals
([semanal.py L5491–5500, L7610–7614](https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/mypy/semanal.py#L5491-L5500)).
So for rakaia, mypy sees `__all__ == ["app", "__version__"]`, and because an
`__all__` exists, it marks every other name non-public (`adjust_public_exports`,
[L905–916](https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/mypy/semanal.py#L905-L916)).
Default mypy still lets you import those names explicitly. `--strict` does not.
And under any settings, `from rakaia import *` brings in only those two names.

**stubtest** compares a static view of a package against the imported runtime
module ([stubtest.rst](https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/docs/source/stubtest.rst)).
The docs present it as a tool for stubs, but it does run on an inline-typed package
and treats the `.py` files as the stubs. On the test packages it reported exactly
this bug: *"`__all__` names exported from the stub do not correspond to the names
exported at runtime … Names exported at runtime but not in the stub: ['Thing',
'f']"*. On rakaia it stops earlier, with *"not checking stubs due to mypy build
errors"* (a missing `uvicorn`, plus mypy errors in `executors.py:237` and
`replay.py:761` that pyright does not report). So it is not usable here without
other work first.

---

## 5. Lazy loading

- **PEP 562** (Final, 3.7) adds module-level `__getattr__`/`__dir__`. Type checkers
  do not run it. A `__getattr__` that a checker can see turns every unknown name into
  its return type, which is how `django_rakaia` ends up giving consumers `Any` (see
  the table).
- **`TYPE_CHECKING` block or `__init__.pyi`.** Both give checkers static bindings.
  A `.pyi` next to the `.py` replaces it completely for type checkers, so every name
  has to be written down twice. typeshed's guidance is that a stub has an `__all__`
  *"if and only if it is also present at runtime"*, with *"identical"* contents
  ([writing_stubs L177–186](https://github.com/python/typing/blob/dc0a8a924c99c6d6ab70f5dc4708e8e4017386ae/docs/guides/writing_stubs.rst#L177-L186)).
  Nothing checks that agreement except stubtest. For an inline-typed package, a stub
  adds no expressive power over a `TYPE_CHECKING` block.
- **SPEC 1** (Scientific Python, endorsed by NumPy, SciPy, scikit-image,
  scikit-learn, NetworkX, IPython and PySAL). It admits that lazy loading means
  *"static type checkers … will not be able to infer the types"*. Its remedy is an
  `__init__.pyi` using either `from .edges import sobel as sobel` (*"necessary due
  to PEP 484"*) or a literal `__all__`, with `lazy.attach_stub(__name__, __file__)`
  in the `.py`
  ([spec-0001 L229–287](https://github.com/scientific-python/specs/blob/7d99f8e601821f030f5c6b9cb492011eaf6521dc/spec-0001/index.md#L229-L287)).
  `attach_stub` parses the `.pyi` with `ast` and accepts only `from .x import …`
  (level 1, no star)
  ([lazy_loader L335–385](https://github.com/scientific-python/lazy-loader/blob/e162cd178836ed5590e17457d1afa9a555862925/src/lazy_loader/__init__.py#L335-L385)).
  Because one file feeds both the checker and the runtime, they cannot drift apart.
- **PEP 690** (implicit, global lazy imports) was **rejected** on 2022-12-02. The
  Steering Council said it would cause *"a split in the community over how imports
  work"*, would mean *"unexpected import related exceptions … at the time of first
  use virtually anywhere"*, and that they *"do not envision the Python language
  transitioning to a world where lazy imports are the default"*
  ([decision](https://discuss.python.org/t/pep-690-lazy-imports-again/19661/26)).
- **PEP 810** (explicit `lazy import` / `lazy from … import …`) has **Status: Final,
  Python-Version: 3.15**, and was accepted 2025-11-03
  ([PEP 810 header](https://github.com/python/peps/blob/e449c7e446faec3ecf953e83fbe677b771bf5d7c/peps/pep-0810.rst#L11-L17)).
  The syntax only works at module level, not inside `try` blocks, and not with `*`.
  The PEP says *"type checkers … may treat `lazy` imports as ordinary imports for
  name resolution"* (L945–951). For older versions it offers `__lazy_modules__`,
  which makes imports *eager* below 3.15 (L1353–1369). PEP 790 scheduled 3.15.0 for
  2026-10-01, but on 2026-10-02 python.org's release API listed nothing later than
  3.15.0rc2. **Whether 3.15.0 has shipped is unverified.** Whether pyright and mypy
  support the `lazy` keyword is also unverified; I did not test it.

---

## 6. What major projects do

| Project | Root mechanism | How names are declared to checkers | Type checking in CI |
|---|---|---|---|
| **NumPy** | Eager names; submodules lazy through `__getattr__`; runtime `__all__` built from set unions ([L676–740](https://github.com/numpy/numpy/blob/bcf9e89bf425a5716e8b310ddff8a1ab17fbb12e/numpy/__init__.py#L676-L740)) | Hand-written 500 KB `__init__.pyi` with a literal `__all__` and `X as X` ([L183–190, L694](https://github.com/numpy/numpy/blob/bcf9e89bf425a5716e8b310ddff8a1ab17fbb12e/numpy/__init__.pyi#L694)) | `spin stubtest` ([stubtest.yml](https://github.com/numpy/numpy/blob/bcf9e89bf425a5716e8b310ddff8a1ab17fbb12e/.github/workflows/stubtest.yml)); mypy, `pyrefly check`, `pyrefly coverage check --public-only` ([typecheck.yml L82–102](https://github.com/numpy/numpy/blob/bcf9e89bf425a5716e8b310ddff8a1ab17fbb12e/.github/workflows/typecheck.yml#L82-L102)). No `--verifytypes`. |
| **SciPy** | Submodules lazy through `__getattr__`; `__all__ = submodules + [...]` ([L77–111](https://github.com/scipy/scipy/blob/3d18c8f1316850a21ee462bdd7268965c2509d90/scipy/__init__.py#L77-L111)) | **No `py.typed`** anywhere in the tree, so the library-interface rules do not apply | No type checking found in `lint.yml` |
| **scikit-image** | `lazy_loader.attach_stub` in every subpackage ([filters/\_\_init\_\_.py](https://github.com/scikit-image/scikit-image/blob/533b7694d2004ae84e49e2cfd0bcfc5f8e562f22/src/skimage/filters/__init__.py)) | `.pyi` with a literal `__all__` and plain imports ([filters/\_\_init\_\_.pyi](https://github.com/scikit-image/scikit-image/blob/533b7694d2004ae84e49e2cfd0bcfc5f8e562f22/src/skimage/filters/__init__.pyi)); the root `.pyi` uses `__all__ = _submodules + [...]`, which is not a spec form | Generates stubs with docstub; stubtest is explicitly *"not needed yet"* ([typing.yml L45–66](https://github.com/scikit-image/scikit-image/blob/533b7694d2004ae84e49e2cfd0bcfc5f8e562f22/.github/workflows/typing.yml#L45-L66)) |
| **Pydantic** | `_dynamic_imports` dict plus `__getattr__` ([L252, L430–460](https://github.com/pydantic/pydantic/blob/29933d1c8cfc882a7640f4d831b09381b39aa311/pydantic/__init__.py#L430-L460)) | `TYPE_CHECKING` block of **plain** imports and `import *`, **plus a hand-written literal `__all__` tuple** ([L11–74](https://github.com/pydantic/pydantic/blob/29933d1c8cfc882a7640f4d831b09381b39aa311/pydantic/__init__.py#L11-L74)). It works because of the literal `__all__`, not the import form. | Consumer-style files under `tests/typechecking` checked with pyright (pinned to `1.1.413`) and mypy ([ci.yml L845–849](https://github.com/pydantic/pydantic/blob/29933d1c8cfc882a7640f4d831b09381b39aa311/.github/workflows/ci.yml#L845-L849)); [`test_exports.py`](https://github.com/pydantic/pydantic/blob/29933d1c8cfc882a7640f4d831b09381b39aa311/tests/test_exports.py) checks the runtime side |
| **httpx** | Eager `from ._api import *` and so on | Literal `__all__` ([\_\_init\_\_.py](https://github.com/encode/httpx/blob/b5addb64f0161ff6bfe94c124ef76f6a1fba5254/httpx/__init__.py)) | `mypy` on the source ([scripts/check L13](https://github.com/encode/httpx/blob/b5addb64f0161ff6bfe94c124ef76f6a1fba5254/scripts/check#L13)) |
| **attrs** | Eager names; `__getattr__` only for lazy submodules and `__version__` ([L85–116](https://github.com/python-attrs/attrs/blob/a602f78ff670f22be90fb8aea501a6cc30e10fdd/src/attr/__init__.py#L85-L116)) | Literal `__all__` in `.py`; `.pyi` uses `X as X` throughout ([attr/\_\_init\_\_.pyi L17–30](https://github.com/python-attrs/attrs/blob/a602f78ff670f22be90fb8aea501a6cc30e10fdd/src/attr/__init__.pyi#L17-L30)) | mypy on the stubs and `typing_tests`; pyright, ty and pyrefly envs ([tox.ini L10–34, L126–131](https://github.com/python-attrs/attrs/blob/a602f78ff670f22be90fb8aea501a6cc30e10fdd/tox.ini#L10-L34)). No `--verifytypes`. |
| **Django** | Root exports only `VERSION`, `__version__` and `setup` ([\_\_init\_\_.py](https://github.com/django/django/blob/80ea222c8af501031b2eae4f7d9fdbf16cb507a7/django/__init__.py)); `db.models` builds `__all__` from submodule `__all__`s | **No `py.typed`**; types come from the separate django-stubs, which uses `X as X` ([models/\_\_init\_\_.pyi](https://github.com/typeddjango/django-stubs/blob/de0b49c8fb5d82bb2d5ab86ab4e4a92430a3893b/django-stubs/db/models/__init__.pyi)) | n/a |
| **typeshed** | n/a | Stubs follow PEP 484's `X as X`; `__all__` only when the runtime has one, and identical to it ([CONTRIBUTING L325–338](https://github.com/python/typeshed/blob/b932d8ce0f893d7d9ef167f0379dcaf9f48bf70b/CONTRIBUTING.md#L325-L338)) | stubtest is typeshed's main tool |

Across these projects, every one that ships `py.typed` and loads names lazily
writes the public list out statically: as a literal `__all__`, as `X as X`, or both.
None of them derives `__all__` from a runtime data structure in a file that checkers
read. None of their CI configs uses `--verifytypes`. The ones that check what a
consumer sees do it by type-checking consumer code (pydantic, attrs) or with
stubtest (NumPy).

---

## 7. Experiment

I built one package for each style, installed it non-editable into a clean venv
alongside the rakaia 0.7.0 wheel, and type-checked
`from pkg import Thing, f; pkg.Thing`. "✗" means the names were rejected. Scratch
files are under the session scratchpad and are not committed.

| Style | pyright 1.1.409 | pyright 1.1.411 | mypy `--strict` | mypy `import *` |
|---|---|---|---|---|
| **rakaia 0.7.0 wheel** (also tested from PyPI) | ✗ all names | ✓ | ✗ all names | ✗ only `app`, `__version__` |
| A: plain `TYPE_CHECKING` imports + dynamic `__all__` (rakaia's pattern) | ✗ | ✓ | ✗ | ✗ nothing |
| B: `y as y` in `TYPE_CHECKING` + dynamic `__all__` | ✓ | ✓ | ✓ | ✗ nothing |
| C: plain `TYPE_CHECKING` imports + **literal `__all__`** | ✓ | ✓ | ✓ | ✓ |
| D: `__getattr__` only (django_rakaia's pattern) | ✓ but `Any` | ✓ but `Any` | ✓ but `Any` | — |
| E: `__init__.pyi` with `y as y` | ✓ | ✓ | ✓ | — |
| F: `__init__.pyi` with literal `__all__` (SPEC 1) | ✓ | ✓ | ✓ | — |
| H: eager plain import, no `__all__` | ✗ | ✗ | ✗ | — |
| rakaia wheel with only the imports changed to `y as y` | ✓ | — | ✓ | — |

Default mypy (without `--strict`) accepts A and the rakaia wheel. partisipa-import
runs default mypy 1.19.0 with no pyright (`origin/main` at 505dadc5, read
2026-10-02), so it would not hit this.

---

## 8. Recommendation for rakaia

**Smallest correct fix: a literal `__all__`.** Write
`__all__ = ["AnyEffect", ..., "app", "__version__"]` out by hand. Keep `_EXPORTS`
for `__getattr__`, and leave the `TYPE_CHECKING` block as it is. Then add an
assertion to `test_public_api.py` that `set(__all__) == set(_EXPORTS) | {"app",
"__version__"}`. This is row C, which passes every checker tested, including
`import *`. It makes the warning go away, so `reportUnsupportedDunderAll` can become
`"error"` in `[tool.pyright]` and our own CI fails if anyone reintroduces this.
Ruff's `RUF022` can keep the list sorted. Do the same for `django_rakaia`, which
also needs a `TYPE_CHECKING` block, because today its consumers get `Any` for every
name (row D). The cost is one more place to write a name, and that should be added
to CLAUDE.md's "Adding a public name touches five places" list.

The `y as y` form (row B) is the alternative the ruff comment already turned down.
It fixes named imports but not `import *` under mypy, and it leaves the warning in
place. It is not enough on its own.

**Regression check.** Add a CI job, run on the built wheel and not on `src/`, that:

1. runs `uv build`, then installs the wheel non-editable into a fresh venv with no
   `src/` on the path;
2. generates `consumer.py` from the installed package's runtime `__all__`
   (`from rakaia import <every name>` and `reveal_type` on each), so the check cannot
   fall out of step with the list;
3. runs pyright at the locked version **and** at a floor older than 1.1.410 (for
   example 1.1.409), plus `mypy --strict`, all on `consumer.py`.

Any of those three runs would have failed on 0.7.0. `pyright --verifytypes rakaia
--ignoreexternal` with a check that every `__all__` name appears in the
`--outputjson` symbol list would also have caught it, but the score alone would not
(see section 3). stubtest would catch it too, once the mypy errors that currently
stop it are fixed.

**Keep lazy loading.** Its purpose is measured and real: 80 ms down to 3 ms, and no
store allocated just for importing the package. The bug is in how the names are
declared, which a literal `__all__` fixes without giving up laziness. A `.pyi` stub
(rows E and F) would also work, but it hides `__init__.py` from checkers, doubles
the bookkeeping, and adds nothing for an inline-typed package. PEP 810 is the
long-term way out: `lazy from .store import StreamStore as StreamStore` would be
lazy at runtime and visible to checkers, with no `_EXPORTS` table at all. But
rakaia targets Python 3.10, and `__lazy_modules__` would make every import eager
below 3.15. That would undo #240 for most users. `app` and the `replay`
name collision would still need special handling. Revisit when 3.15 is the oldest
supported version.

---

## Unverified

- That pyright **1.1.411** reports `reportPrivateImportUsage` for rakaia 0.7.0. I
  could not reproduce it with the wheel built from `main` or with the PyPI 0.7.0
  wheel, in either `standard` or `strict` mode. I could reproduce it with 1.1.409.
  If a consumer sees it on 1.1.411, check which pyright they actually run (a
  Pylance or basedpyright build, or a different pin) before assuming the version.
- Whether Python 3.15.0 has been released (it was scheduled for 2026-10-01).
- Type-checker support for PEP 810's `lazy` syntax.

## Sources

- Typing spec, distributing: https://github.com/python/typing/blob/dc0a8a924c99c6d6ab70f5dc4708e8e4017386ae/docs/spec/distributing.rst (rendered: https://typing.python.org/en/latest/spec/distributing.html#library-interface-public-and-private-symbols)
- Typing spec, directives: https://github.com/python/typing/blob/dc0a8a924c99c6d6ab70f5dc4708e8e4017386ae/docs/spec/directives.rst
- Writing stubs guide: https://github.com/python/typing/blob/dc0a8a924c99c6d6ab70f5dc4708e8e4017386ae/docs/guides/writing_stubs.rst
- PEP 484: https://peps.python.org/pep-0484/#stub-files · PEP 561: https://peps.python.org/pep-0561/ · PEP 562: https://peps.python.org/pep-0562/
- PEP 690: https://peps.python.org/pep-0690/ · rejection: https://discuss.python.org/t/pep-690-lazy-imports-again/19661/26
- PEP 810: https://peps.python.org/pep-0810/ (pinned: https://github.com/python/peps/blob/e449c7e446faec3ecf953e83fbe677b771bf5d7c/peps/pep-0810.rst) · acceptance: https://discuss.python.org/t/pep-810-explicit-lazy-imports/104131/466
- PEP 790 (3.15 schedule): https://peps.python.org/pep-0790/ · python.org release API: https://www.python.org/api/v2/downloads/release/?is_published=true
- Pyright 1.1.411 docs: https://github.com/microsoft/pyright/blob/1.1.411/docs/typed-libraries.md · https://github.com/microsoft/pyright/blob/1.1.411/docs/configuration.md · https://github.com/microsoft/pyright/blob/1.1.411/docs/command-line.md
- Pyright binder fallback: https://github.com/microsoft/pyright/blob/1.1.411/packages/pyright-internal/src/analyzer/binder.ts#L338-L376 · https://github.com/microsoft/pyright/pull/11396 · https://github.com/microsoft/pyright/releases/tag/1.1.410
- mypy: https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/docs/source/command_line.rst · https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/mypy/semanal.py · https://github.com/python/mypy/blob/05e50c09918549e86b379684dcf27cfd35f880f2/docs/source/stubtest.rst
- SPEC 1: https://github.com/scientific-python/specs/blob/7d99f8e601821f030f5c6b9cb492011eaf6521dc/spec-0001/index.md · lazy_loader: https://github.com/scientific-python/lazy-loader/blob/e162cd178836ed5590e17457d1afa9a555862925/src/lazy_loader/__init__.py
- Project sources and CI: as linked in the section 6 table (NumPy `bcf9e89b`, SciPy `3d18c8f1`, scikit-image `533b7694`, pydantic `29933d1c`, httpx `b5addb64`, attrs `a602f78f`, Django `80ea222c`, django-stubs `de0b49c8`, typeshed `b932d8ce`)
- typeshed CONTRIBUTING: https://github.com/python/typeshed/blob/b932d8ce0f893d7d9ef167f0379dcaf9f48bf70b/CONTRIBUTING.md
