# Deadwood-AI API

This is the repository for the Deadwood-AI API. This API is used to
upload user-contributed drone or airborne imagary to the Deadwood-AI platform, either
by the [deadtrees.earth frontend](https://deadtrees.earth), or by a local upload script.

To use the API locally, you need to install the dependencies and start the ASGI server:

```bash
pip install -r requirements.txt
python run.py
```

Then, open the swagger-ui docs at `http://localhost:8000/docs` (or alternatively ReDoc at `http://localhost:8000/redoc`).