# CLI Interface

import click
from datetime import date, timedelta
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

@cli.command()
@click.option("--start-date", type=click.DateTime(formats=['%Y-%m-%d']), required=True, help="Start date")
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]), required= True, help="End date")
def backfill(start_date, end_date):
    """Backfill data for a date range"""
    start = start_date.date()
    end = end_date.date()

    total_days = (end - start).days + 1
    click.echo(f"Backfilling {total_days} days: {start} to {end}")

    pipeline = DailyPipeline()
    current = start

    while current <= end:
        click.echo(f"Processing {current}...")
        pipeline.run(current)
        current += timedelta(days=1)

    click.echo(f"Backfill complete: {total_days} days processed")

if __name__ == "__main__":
    cli()