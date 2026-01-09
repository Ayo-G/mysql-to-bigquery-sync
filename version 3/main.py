from pipeline_entry import handler

def app(request):
    """Cloud Functions Gen 2 HTTP entrypoint."""
    result = handler(request)
    return {"message": result}, 200
