# Snakemake storage plugin: zenodo

A Snakemake storage plugin for reading from and writing to zenodo.org.
The plugin takes queries of the form `zenodo://record/121246/path/to/file.txt` for
downloading files (here from record `121246`), and 
`zenodo://deposition/121246/path/to/file.txt` for uploading files
to an existing deposition (i.e. an unpublished and therefore still writable record).