IF "%1"=="" (
    echo "Repository name is required"
) ELSE (
    pip install --upgrade build twine
    python -m build  || goto :error
    python -m twine upload --repository "%1" dist/*
    RMDIR /Q/S dist
   :error
   echo Failed with error #%errorlevel%.
)