"""
Command line Interface for impc_etl package.
"""

import os
from pathlib import Path
import subprocess

import click


@click.group()
def cli():
    pass


@click.command()
@click.argument("input_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True))
@click.argument("output_dir", type=click.Path(file_okay=False, dir_okay=True))
@click.option("--batch-size", default=1000, type=int, help="Batch size for processing (default: 1000)")
@click.option("--full-suffix", default="_full", type=str, help="Full size image suffix")
@click.option("--thumbnail-suffix", default="_thumbmail", type=str, help="Thumbnail suffix")
@click.option("--thumbnail-width", default=200, type=int, help="Width of thumbnail (default: 200)")
@click.option("--thumbnail-quality", default=80, type=int, help="Quality of thumbnail (default: 80)")
def generate_jpegs(input_dir, output_dir, batch_size, full_suffix, thumbnail_suffix, thumbnail_width, thumbnail_quality):
    """Generate JPEG images from an images directory which can have different sizes and file formats.
    For each image found on the input it should generate a JPEG image with a full resolution, and one image to be used as thumbnail.
    - Requires an input directory and an output directory.
    - Should allow to define suffixes for naming the full resolution and the thumbnail images to be generated.
    """
    if os.path.exists(output_dir):
        click.echo(f"Error: The output directory '{output_dir}' already exists.", err=True)
        raise SystemExit(1)
    
    os.mkdir(output_dir)

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    manifest = output_dir / "manifest.tsv"
    total_files = 0

    click.echo(f"Generating list of input files for input directory {input_dir}")
    with open(manifest.resolve(), "w", encoding="utf-8") as f:
        for input_path in input_dir.rglob("*"):
            if input_path.is_file():
                output_path = output_dir / input_path.relative_to(input_dir)
                f.write(f"{input_path.resolve()}\t{output_path.resolve()}\n")
                total_files += 1

    click.echo(f"Generating JPEG files from {input_dir} to {output_dir}")
    for batch_start in range(0, total_files, batch_size):
        click.echo(f"Submit {batch_start} of {total_files}")
        result = subprocess.run(
            [
                "python3",
                "../jobs/transform/images_jpg_generation.py",
                f"--manifest={manifest.resolve()}",
                f"--batch-from={batch_start}",
                f"--batch-to={batch_start + batch_size}",
                f"--full-suffix={full_suffix}",
                f"--thumbnail-suffix={thumbnail_suffix}",
                f"--thumbnail-width={thumbnail_width}",
                f"--thumbnail-quality={thumbnail_quality}"
            ],
            check=True,
            capture_output=True,
            text=True
        )


cli.add_command(generate_jpegs)

if __name__ == "__main__":
    cli()
