import azure.functions as func
from data_analysis import run_analysis

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Call your main analysis function
        run_analysis()

        return func.HttpResponse(
            "Data processed and outputs uploaded to Azure Blob Storage.",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            f"Error: {str(e)}",
            status_code=500
        )
