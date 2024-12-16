from aiohttp import web
import aiohttp_cors
import json
from datetime import datetime
from utils import (
    create_database_connection,
    get_filing_text_by_accession_number,
    decode_blob,
    encode_blob,
    custom_fetch_and_save_filings,
    fetch_ticker_to_cik_mapping,
    DownloadMetadata
)
from pathlib import Path
from typing import Optional, Dict, Any
from summary import summarize_filing
from cleaner import clean_and_store_filing
from parsing import retrieve_and_save_parsed_blob

routes = web.RouteTableDef()

async def error_response(message: str, status: int = 400) -> web.Response:
    return web.Response(
        text=json.dumps({"error": message}),
        status=status,
        content_type='application/json'
    )

async def success_response(data: Any) -> web.Response:
    return web.Response(
        text=json.dumps(data),
        content_type='application/json'
    )

# Filing Management Endpoints
@routes.get('/api/filings')
async def get_filings(request):
    connection = create_database_connection()
    if not connection:
        return await error_response("Database connection failed", 500)
    
    try:
        # Parse query parameters
        company = request.query.get('company')
        form_type = request.query.get('form')
        start_date = request.query.get('start_date')
        end_date = request.query.get('end_date')
        limit = request.query.get('limit', '100')
        
        # Build query
        query = """
            SELECT 
                company_identifier,
                form,
                accession_number,
                filing_date,
                report_date,
                file_url
            FROM sec_filings
            WHERE 1=1
        """
        params = []
        
        if company:
            query += " AND company_identifier = %s"
            params.append(company)
        if form_type:
            query += " AND form = %s"
            params.append(form_type)
        if start_date:
            query += " AND filing_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND filing_date <= %s"
            params.append(end_date)
            
        query += " ORDER BY filing_date DESC LIMIT %s"
        params.append(int(limit))
        
        with connection.cursor(dictionary=True) as cursor:
            cursor.execute(query, tuple(params))
            filings = cursor.fetchall()
            
            # Process dates for JSON serialization
            for filing in filings:
                if filing.get('filing_date'):
                    filing['filing_date'] = filing['filing_date'].isoformat()
                if filing.get('report_date'):
                    filing['report_date'] = filing['report_date'].isoformat()
            
            return await success_response({"filings": filings})
    except Exception as e:
        return await error_response(str(e), 500)
    finally:
        connection.close()

@routes.get('/api/filing/{accession_number}')
async def get_filing(request):
    accession_number = request.match_info['accession_number']
    connection = create_database_connection()
    
    if not connection:
        return await error_response("Database connection failed", 500)
    
    try:
        content = get_filing_text_by_accession_number(accession_number, connection)
        if content:
            return await success_response({"content": content})
        return await error_response("Filing not found", 404)
    except Exception as e:
        return await error_response(str(e), 500)
    finally:
        connection.close()

@routes.post('/api/filings/download')
async def download_filings(request):
    try:
        data = await request.json()
        ticker = data.get('ticker')
        form_type = data.get('form_type', '10-K')
        limit = int(data.get('limit', 1))
        after_date = datetime.strptime(data.get('after_date', '2023-01-01'), '%Y-%m-%d').date()
        before_date = datetime.strptime(data.get('before_date', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d').date()
        
        if not ticker:
            return await error_response("Ticker is required")
        
        cik = fetch_ticker_to_cik_mapping(ticker)
        if not cik:
            return await error_response(f"Could not find CIK for ticker {ticker}")
        
        metadata = DownloadMetadata(
            download_folder=Path("/tmp"),
            form=form_type,
            cik=cik,
            ticker=ticker,
            limit=limit,
            before=before_date,
            after=after_date
        )
        
        connection = create_database_connection()
        if not connection:
            return await error_response("Database connection failed", 500)
        
        try:
            downloaded = custom_fetch_and_save_filings(
                metadata,
                "MyCompanyName/1.0 (contact@example.com)",
                connection
            )
            return await success_response({
                "message": f"Successfully downloaded {downloaded} filings",
                "downloaded_count": downloaded
            })
        finally:
            connection.close()
            
    except Exception as e:
        return await error_response(str(e), 500)

# Text Processing Endpoints
@routes.post('/api/filing/{accession_number}/parse')
async def parse_filing(request):
    accession_number = request.match_info['accession_number']
    try:
        retrieve_and_save_parsed_blob(accession_number)
        return await success_response({"message": f"Successfully parsed filing {accession_number}"})
    except Exception as e:
        return await error_response(str(e), 500)

@routes.post('/api/filing/{accession_number}/clean')
async def clean_filing(request):
    accession_number = request.match_info['accession_number']
    try:
        success = clean_and_store_filing(accession_number)
        if success:
            return await success_response({"message": f"Successfully cleaned filing {accession_number}"})
        return await error_response("Failed to clean filing")
    except Exception as e:
        return await error_response(str(e), 500)

@routes.post('/api/filing/{accession_number}/summarize')
async def summarize_filing_endpoint(request):
    accession_number = request.match_info['accession_number']
    try:
        summary_blob = summarize_filing(accession_number)
        if summary_blob:
            summary_text = decode_blob(summary_blob)
            return await success_response({
                "message": f"Successfully summarized filing {accession_number}",
                "summary": summary_text
            })
        return await error_response("Failed to generate summary")
    except Exception as e:
        return await error_response(str(e), 500)


def init_app():
    app = web.Application()
    app.add_routes(routes)
    
    cors = aiohttp_cors.setup(app, defaults={
        "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*"
        )
    })
    
    for route in list(app.router.routes()):
        cors.add(route)
    
    return app

if __name__ == '__main__':
    app = init_app()
    web.run_app(app, port=8503)
