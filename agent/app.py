
import sys
import os
from agent.sql_agent import answer_question
from agent.config import OLLAMA_MODEL

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import streamlit as st

st.set_page_config(page_title="NYC Taxi Analytics Agent", layout="wide")

st.title("🚖 NYC Taxi Analytics Agent")
st.caption(f"LLM: {OLLAMA_MODEL} | Source: mart tables in DuckDB")

if "history" not in st.session_state:
    st.session_state.history = []

user_input = st.chat_input("Ask a business question about the taxi marts...")

for item in st.session_state.history:
    with st.chat_message("user"):
        st.write(item["question"])
    with st.chat_message("assistant"):
        st.write(item["summary"])
        with st.expander("SQL"):
            st.code(item["sql"], language="sql")
        with st.expander("Result"):
            st.dataframe(item["df"], use_container_width=True)

if user_input:
    with st.chat_message("user"):
        st.write(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Thinking, generating SQL, and querying..."):
            try:
                sql, df, summary = answer_question(user_input)
                st.write(summary)
                with st.expander("SQL"):
                    st.code(sql, language="sql")
                with st.expander("Result"):
                    st.dataframe(df, use_container_width=True)

                st.session_state.history.append({
                    "question": user_input,
                    "sql": sql,
                    "df": df,
                    "summary": summary
                })
            except Exception as e:
                st.error(f"Error: {e}")
                