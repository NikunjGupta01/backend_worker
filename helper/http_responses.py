from fastapi.responses import JSONResponse


class HttpResponses:

    @staticmethod
    def success(data=None, message=None, code=200):
        return JSONResponse(
            content={"success": True, "message": message, "data": data},
            status_code=code,
        )

    @staticmethod
    def error(data=None, message=None, code=400):
        return JSONResponse(
            content={"success": False, "message": message, "data": data},
            status_code=code,
        )
