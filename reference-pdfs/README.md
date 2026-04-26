# Reference PDFs (local mirror)

These files were downloaded from the same URLs linked in `index.html` (Appendix / footnotes) so you can open them locally or run text search (`pdftotext`, Preview, etc.).

| # | File | Source URL |
|---|------|------------|
| 01 | `01-JCC-East-Bay-Project-CEQA-Analysis.pdf` | [CEQA JCC East Bay (S3)](https://cao-94612.s3.us-west-2.amazonaws.com/documents/JCC-East-Bay-Project-CEQA-Analysis.pdf) |
| 02 | `02-Alexandria-cut-through-research-2020-05-07.pdf` | [Alexandria cut-through report](https://media.alexandriava.gov/docs-archives/tes/info/city-of-alexandria-cut-through-research---report-5.7.2020%3Dfinal.pdf) |
| 03 | `03-Berkeley-Bicycle-Boulevard-Design-Tools-and-Guidelines.pdf` | [Berkeley CA PDF](https://berkeleyca.gov/sites/default/files/2022-02/Bicycle-Boulevard-Design-Tools-and-Guidelines.pdf) |
| 04 | `04-Oakland-81st-Ave-demonstration-project-summary-2024-12-17.pdf` | [Oakland 81st Ave summary](https://www.oaklandca.gov/files/assets/city/v/1/transportation/documents/projects/calm-east-oakland-streets/81st-ave-demonstration-project-summary-12-17-24.pdf) |
| 05 | `05-Ney-Avenue-Traffic-Calming-Study-Memo-Only.pdf` | [Ney Avenue memo (S3)](https://cao-94612.s3.amazonaws.com/documents/Ney-Avenue-Neighborhood-Traffic-Calming-Study-Memo-Only.pdf) |

**Not mirrored here (not PDFs on the page):** Google Sheets for traffic treatments, ScienceDirect articles, NACTO web pages, YouTube, etc.—those are links only.

Re-download (from repo root):

```bash
cd reference-pdfs && \
curl -fsSL -O <paste URL> && \
ls -la
```

**Volume implications (quotes & caveats from these PDFs):** see [`VOLUME-IMPLICATIONS-INDEX.md`](VOLUME-IMPLICATIONS-INDEX.md).  
To regenerate searchable text: `for f in 0*.pdf; do pdftotext "$f" _extracted/"${f%.pdf}.txt"; done` (the `_extracted/` folder is gitignored).

PDFs in this folder are listed in `.gitignore` to keep the git repo small; keep them on disk locally or store elsewhere if the team needs them in version control (e.g. Git LFS).
