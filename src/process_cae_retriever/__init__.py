from dotenv import load_dotenv
load_dotenv()

from .cae import _CAERetriever
import importlib.util
if importlib.util.find_spec('pygeoapi') is not None:
    from .cae import CAERetrieverProcessor

from .main import run_cae_retriever
from .utils.strings import parse_event