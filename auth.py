"""
Authentication Module for Vendor Upload Portal

This module handles user authentication for the portal.
"""

import streamlit as st

def authenticate_user():
    """Simple authentication mechanism for demonstration purposes."""
    st.sidebar.title("Login")
    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")

    if st.sidebar.button("Login"):
        if username == "vendor" and password == "password":
            st.sidebar.success("Logged in as vendor")
            return True
        else:
            st.sidebar.error("Invalid username or password")
            return False

    return False