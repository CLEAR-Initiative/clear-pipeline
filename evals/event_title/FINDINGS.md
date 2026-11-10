# Event titles keep the places the signals name (#1258)

Ticket: [#1258](https://www.exponential.im/w/clear/products/clear/tickets/1258).

## Result

`rewrite-v4`, Claude Haiku 4.5, 3 runs per case, 10 Nov 2026: **18 of 18 pass**.

| Case | Source | Title (run 1) |
| --- | --- | --- |
| `three-villages` | real: dev event `cmvuv8hya00051jogybdegcfn` | Battles in Mazbud (مزبد), Aruri (عروري) and Kamo (كامو), North Darfur |
| `al-kanabi` | real: dev event `cmvzgvxui00i31js5wz9xln9a` | Explosions and forced displacement in Al-Kanabi (الكنابي), Northern |
| `state-only` | authored | Cholera outbreak in North Kordofan |
| `english-only` | authored | Flash floods in Tendalti, White Nile |
| `two-signals-different-villages` | authored | Battles in South Kordofan |
| `two-signals-same-town` | authored | Battles in Al-Dalanj (الدلنج), South Kordofan |

With `rewrite-v3` the two real titles were "Battles in North Darfur" and
"Explosions and forced displacement in Northern Sudan".

## What each case checks

- The title names each place the signals name, and the admin area for context.
- Each Arabic spelling in parentheses occurs in the signal text, letter for letter.
- No Arabic outside parentheses.
- No parentheses when the text is English, or names only a state.
- Two signals that name different places: the area alone.
- At most 100 characters.

## Two things the first run showed

1. The Arabic spellings came in one run of three for the three-village post.
   The rule now says ALWAYS, with an example of two places. After that: 3 of 3.
2. For two signals that name different villages, the model named both. The
   rule now says: name a place only when every signal names it. After that:
   3 of 3 give the area alone.

## Decisions

- **Length.** The prompt asks for 80 characters of English. Arabic spellings
  come on top. `clean_event_title` removes every Arabic spelling when the
  title is longer than 100 characters with them.
- **The code check** (`clean_event_title`) removes an Arabic spelling that
  occurs in no signal's text, and invisible direction marks.
- **English place names are not checked in code.** A transliteration cannot
  be matched to Arabic text by a rule. The prompt forbids added places, and
  these cases check it by reading.

## Not checked

- Whether الكنابي is one place. *Kanabi* is the plural of *kambo* (a
  farm-worker settlement), so the post can mean the kanabi of the state. The
  ticket lists it for a Sudanese colleague.
- English spellings vary between runs ("Al-Dalanj", "Al-Dilanj"). The Arabic
  spelling beside it is what a reader checks.
- Six cases are a smoke test, not a measurement.

## Run it

```bash
LLM_SIGNAL_PROVIDER=... LLM_SIGNAL_MODEL=... LLM_SIGNAL_API_KEY=... \
  uv run --frozen python evals/event_title/eval.py 3
```
