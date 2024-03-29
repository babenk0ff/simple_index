from pathlib import Path

from dotenv import dotenv_values

config = dotenv_values(Path(__file__).parent.parent.resolve() / '.env')
