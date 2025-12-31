# `snakemake` conversion of the R targets pipeline
## Overview
This directory contains scripts and workflow files to convert the R-based targets pipeline into a `snakemake` workflow. The goal is to replicate the functionality of the original R code while leveraging the capabilities of `snakemake` for workflow management.


## How to run
To execute the workflow, ensure you have `snakemake` installed and run the following command in the terminal:

```bash
snakemake --cores <number_of_cores>
```

Replace `<number_of_cores>` with the number of CPU cores you wish to utilize for parallel processing.

You can also specify a particular target file to generate. For example, to create the `features_monitors_correct.parquet` file, use:

```bash
snakemake --cores 12 -j 1 /mnt/u/interim_results/features_monitors_correct.parquet
```