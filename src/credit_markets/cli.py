# CLI Interface

import click
from datetime import date
from credit_markets.pipeline.daily import DailyPipeline

@click.group()
def cli():
    """Credit Markets Pipeline CLI"""
    pass
@cli.command()
@click.option("--target-date", type=click.DateTime(formats=["%Y-%m-%d"]), default=None, help="Date to run pipeline for")
def run(target_date):
    """Run the daily pipeline"""

    if target_date is None:
        run_date = date.today()
    else:
        run_date = target_date.date()

    click.echo(f"Running pipeline for {run_date}")

    pipeline = DailyPipeline()
    results = pipeline.run(run_date)

    click.echo(f"FRED: {results['fred']['silver_rows']} rows")
    click.echo(f"SEC: {results['sec']['silver_rows']} rows")

if __name__ == "__main__":
    cli()