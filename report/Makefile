report.pdf: report.md report.bib
	docker run --rm --volume "$(PWD):/data" --entrypoint "/data/pandoc.sh" pandoc/latex
	mv -f report.pdf ../
