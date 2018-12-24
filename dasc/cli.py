# -*- coding: utf-8 -*-

"""Console script for dasc."""
import os
import sys
import click

from dasc.main import Server


@click.command()
@click.option('--dir', default='./', type=click.Path(exists=True), help='handler\'s path')
@click.option('--bind', default='tcp://*:5555', help='bind to, tcp|inproc')
def main(*args, **kwargs):
    handler_path = os.path.abspath(kwargs['dir'])
    sys.path.append(handler_path)
    Server(client_url=kwargs['bind'], handler_path=handler_path).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
