# AskDBCloud
 
#### How to run:
1. Install pipenv
2. Clone the repo
3. Execute pipenv shell & pipenv install
4. Rename .streamlit/secrets.example.toml to .streamlit/secrets.toml and fill the required secrets
5. Edit table name list you wish to crawl in core/crawl_metadata.py (This will be improved by making it dynamic)
6. Run crawl_metadata.py
7. Run "streamlit run streamlit_run.py"