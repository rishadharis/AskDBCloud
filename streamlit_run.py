import streamlit as st
from streamlit_chat import message
import time
from core.run import ask_redshift
from streamlit import secrets

def main():
    # Configure page with dark theme
    st.set_page_config(
        page_title="Ask Your Data - Redshift Edition",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'Get Help': 'https://github.com/yourusername/yourrepo',
            'Report a bug': "https://github.com/yourusername/yourrepo/issues",
            'About': "# Ask Your Data - Redshift Edition"
        }
    )

    # Set theme config to dark mode
    st.markdown("""
        <script>
            var elements = window.parent.document.getElementsByTagName('section');
            elements[0].classList.add('dark');
        </script>
        """, unsafe_allow_html=True)

    # Set page title and subtitle
    st.title("Ask Your Data - Redshift Edition")
    st.markdown("""
                An GenAI app to interact with Redshift Database\n
                **By : Rishad**
                """)
    
    # Initialize chat history in session state if it doesn't exist
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Create the main input and button
    question = st.text_input("Type your question here...")
    
    # Create a button with spinner
    if st.button("Ask", type="primary") and question: 
        st.session_state.messages.append({"role": "user", "content": question})
        with st.spinner("Processing your question..."):
            response = ask_redshift(question)
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response
            })

    # Display chat messages
    chat_container = st.container()
    with chat_container:
        for i, msg in enumerate(st.session_state.messages):
            if msg["role"] == "user":
                message(msg["content"], is_user=True, key=f"msg_{i}")
            else:
                message(msg["content"], is_user=False, key=f"msg_{i}")
            
    # Create the sidebar
    with st.sidebar:
        st.title("Available Tables")
        
        # Add "Crawl Tables From Redshift" button
        st.button("Crawl Tables From Redshift", type="primary")
        
        # Add search box
        st.text_input("Search tables...")
        
        # Add "Crawled Tables (3)" header
        st.subheader("Crawled Tables (3)")
        
        # Add example tables with schema and timestamp
        tables = [
            {
                "name": "dm_sales",
                "schema": "lrt_demo",
                "last_crawled": "2024-12-19 10:30:00"
            },
            {
                "name": "dm_incident_maintenance", 
                "schema": "lrt_demo",
                "last_crawled": "2024-12-19 09:45:00"
            },
            {
                "name": "dm_route",
                "schema": "lrt_demo", 
                "last_crawled": "2024-12-19 08:15:00"
            },
            {
                "name": "dm_route_performance_metrics",
                "schema": "lrt_demo", 
                "last_crawled": "2024-12-19 08:15:00"
            },
            {
                "name": "dm_financial_performance_metrics",
                "schema": "lrt_demo", 
                "last_crawled": "2024-12-19 08:15:00"
            }
        ]
        
        # Display tables
        for table in tables:
            with st.expander(table['name'], expanded=True):
                st.text(f"Schema: {table['schema']}")
                st.text(f"Last crawled: {table['last_crawled']}")

if __name__ == "__main__":
    main()
