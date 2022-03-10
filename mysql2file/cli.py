import click
from colorama import init

from .version import __version__

init(autoreset=True)

@click.command("mysql2file")
@click.version_option(__version__, '-V', '--version', prog_name='mysql2file')
def cli():
    ...


# if __name__ == '__main__':
#     cli()
