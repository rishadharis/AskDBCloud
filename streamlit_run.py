import streamlit as st
from streamlit_chat import message
import time
from core.run import ask_redshift

def main():
    st.set_page_config(page_title="Ask Your Data", layout="wide")
    # Set page title and subtitle
    st.title("Ask Your Data")
    st.markdown("An GenAI app to interact with Redshift Database")
    
    # Add header info
    col1, col2 = st.columns([4, 1])
    with col1:
        st.write("For: LRT")
    with col2:
        st.write("Cloud Kinetics")

    # Initialize chat history in session state if it doesn't exist
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Create the main input and button
    question = st.text_input("Type your question here...")
    
    # Create a button with spinner
    if st.button("Ask") and question: 
        st.session_state.messages.append({"role": "user", "content": question})
        with st.spinner("Processing your question..."):
            response = ask_redshift(question)
            # Add the Q&A pair to chat history
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
        st.title("GenAI Chat Bot")
        
        # Add "Crawl Tables From Redshift" button
        st.button("Cloud Kinetics - LRT")
        
        # Add "Crawled Tables (3)" header
        st.subheader("A Generative AI Chat Bot for LRT Demo, Interacting with Redshift Database")
        
        

if __name__ == "__main__":
    main()
