# -*- coding: utf-8 -*-

"""Console script for mongopie."""
import sys
import click


@click.command()
def main(args=None):
    """Console script for mongopie."""
    click.echo("Replace this message by putting your code into "
               "mongopie.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
