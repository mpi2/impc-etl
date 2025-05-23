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
@click.option("--output-dir", type=str)
@click.option("--batch-size", default=1000, show_default=True, type=int, help="Batch size for processing")
@click.option("--full-suffix", default="", type=str, help="Full size image suffix")
@click.option("--thumbnail-suffix", default="_thumbnail", type=str, help="Thumbnail suffix")
@click.option("--thumbnail-width", default=200, show_default=True, type=int, help="Width of thumbnail")
@click.option("--thumbnail-quality", default=80, show_default=True, type=int, help="Quality of thumbnail")
@click.option("--start-processing",  is_flag=True, show_default=True, default=False, help="Whether to run the processing")
def generate_jpegs(
    input_dir: str,
    output_dir: str | None,
    batch_size: int,
    full_suffix: str,
    thumbnail_suffix: str,
    thumbnail_width: int,
    thumbnail_quality: int,
    start_processing: bool
) -> None:
    """Generate JPEG images from an images directory which can have different sizes and file formats.
    For each image found on the input it should generate a JPEG image with a full resolution, and one image to be used as thumbnail.
    - Requires an input directory and an output directory.
    - Should allow to define suffixes for naming the full resolution and the thumbnail images to be generated.
    """
    if output_dir is None:
        output_dir = input_dir
    else:
        if os.path.exists(output_dir):
            click.echo(f"Error: The output directory '{output_dir}' already exists.", err=True)
            raise SystemExit(1)
        os.mkdir(output_dir )

    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    manifest = output_dir / "manifest.tsv"
    total_files = 0

    click.echo(f"Generating list of input files for input directory {input_dir}")
    with open(manifest.resolve(), "w", encoding="utf-8") as f:
        for input_path in input_dir.rglob("*"):
            if input_path.is_file() and (input_path != manifest):
                output_path = output_dir / input_path.relative_to(input_dir)
                f.write(f"{input_path.resolve()}\t{(output_path.parent/output_path.stem).resolve()}\n")
                total_files += 1

    if start_processing:
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
                # capture_output=True,
                text=True
            )


cli.add_command(generate_jpegs)

if __name__ == "__main__":
    cli()
