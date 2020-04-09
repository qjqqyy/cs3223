#!/usr/bin/env sh

tlmgr install latexmk

pandoc \
  --from=markdown+tex_math_single_backslash \
  --biblatex \
  --standalone \
  --number-sections \
  --pdf-engine=xelatex \
  -o report.tex \
  report.md

latexmk -pdfxe report.tex

latexmk -c
