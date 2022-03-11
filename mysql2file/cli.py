import click
from .version import __version__

@click.command("mysql2file")
@click.version_option(__version__, '-V', '--version', prog_name='mysql2file')
def cli():
    ...

if __name__ == '__main__':
    cli()
