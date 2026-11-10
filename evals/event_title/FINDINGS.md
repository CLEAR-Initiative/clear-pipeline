# Event titles keep the places the signals name (#1258)

Ticket: [#1258](https://www.exponential.im/w/clear/products/clear/tickets/1258).

## Result

`rewrite-v4`, Claude Haiku 4.5, 5 runs per case, 10 Nov 2026: **45 of 45 pass**.

| Case | Source | Location given | Title (most runs) |
| --- | --- | --- | --- |
| `three-villages` | real: dev event `cmvuv8hya00051jogybdegcfn` | North Darfur | Battles in Mazbud (مزبد), Aruri (عروري) and Kamo (كامو), North Darfur |
| `al-kanabi` | real: dev event `cmvzgvxui00i31js5wz9xln9a` | Northern | Explosions and forced displacement in Al-Kanabi (الكنابي), Northern |
| `state-only` | authored | North Kordofan | Cholera outbreak in North Kordofan |
| `english-only` | authored | White Nile | Flash floods in Tendalti, White Nile |
| `one-town-in-a-district` | authored | Um Rawaba | Battles in Wad Ashana (ود عشانا), Um Rawaba |
| `two-signals-same-town` | authored | Um Rawaba | Battles in Al-Sumaih (السميح), Um Rawaba |
| `two-towns-one-district` | authored | Um Rawaba | Battles in Wad Ashana and Al-Sumaih (السميح), Um Rawaba |
| `five-villages-one-district` | authored | Um Rawaba | Battles in Um Rawaba |
| `town-with-the-name-of-its-district` | authored | Dilling | Battles in Dilling (الدلنج), South Kordofan |

With `rewrite-v3` the two real titles were "Battles in North Darfur" and
"Explosions and forced displacement in Northern Sudan".

## Location is the event's district

Grouping keys an event on a district. `group_signal` passes that district's
name as Location (`_event_area_name`), not the place the incoming signal was
geocoded to. Before this, a signal from a second town retitled the whole
event after that town. The last five cases are this path: Location is a
district, and the signals name places inside it.

## What each case checks

- The title names each place the signals name, then Location.
- Each Arabic spelling in parentheses occurs in the signal text, letter for letter.
- No Arabic outside parentheses.
- No parentheses when the text is English, or names only a state.
- Two signals that name different villages: both villages or neither, never one.
- Five signals that name five villages: the district alone.
- At most 100 characters.

## What the runs showed

1. The Arabic spellings came in one run of three for the three-village post.
   The rule now says ALWAYS, with an example of two places. After that: 3 of 3.
2. With a district as Location, the model put the state in its place in 2 runs
   of 3 ("…, North Kordofan"). The rule now says: the last place in the title
   is the area on the Location line, no wider area after it.
3. The model does not follow "name a place only when every signal names it".
   For two villages it names both, which is accurate. The rule is now: the
   places of all the signals, or none. The check is "both or neither".
4. The model does not follow "three places at most". For five villages it
   lists four or five in 5 runs of 5. `clean_event_title` counts the places
   before Location and writes Location alone when there are more than three.
   The five-village case passes because of this code, not because of the prompt.
5. Once in 5 runs the model wrote the word "Location" at the end of the title,
   and once an Arabic spelling after a state. Both are removed in code.
6. Runs differ. Earlier runs of this same prompt lost Al-Kanabi in 1 run of 5
   and the Arabic spelling of Wad Ashana in 1 to 3 runs of 5. A lost spelling
   leaves a correct English title.

## Decisions

- **Length.** The prompt asks for 80 characters of English. Arabic spellings
  come on top. `clean_event_title` removes every Arabic spelling when the
  title is longer than 100 characters with them.
- **The code check** (`clean_event_title`) removes an Arabic spelling that
  occurs in no signal's text, an Arabic spelling after Location at the end
  of the title, the word "Location" at the end, and invisible direction
  marks. It writes Location alone when the title names more than three places.
- **Several signals.** The title names the places of all the signals or none.
  Code cannot check this for two or three places: it cannot tell which signal
  a transliterated name comes from.
- **English place names are not checked in code.** A transliteration cannot
  be matched to Arabic text by a rule. The prompt forbids added places, and
  these cases check it by reading.

## Not checked

- Whether الكنابي is one place. *Kanabi* is the plural of *kambo* (a
  farm-worker settlement), so the post can mean the kanabi of the state. The
  ticket lists it for a Sudanese colleague.
- English spellings vary between runs ("Al-Dalanj", "Al-Dilanj"). The Arabic
  spelling beside it is what a reader checks.
- An event with many signals where the model names two or three of the
  places. The count check does not catch it. No case covers it.
- Nine cases are a smoke test, not a measurement.

## Run it

```bash
LLM_SIGNAL_PROVIDER=... LLM_SIGNAL_MODEL=... LLM_SIGNAL_API_KEY=... \
  uv run --frozen python evals/event_title/eval.py 5
```
