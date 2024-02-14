"""Command-line interface."""

import click


@click.command()
@click.version_option()
def main() -> None:
    """SSB Konjunk."""


if __name__ == "__main__":
    main(prog_name="ssb-konjunk")  # pragma: no cover
