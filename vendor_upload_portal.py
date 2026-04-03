"""
Vendor Upload Portal for Carolina Compliance Solutions

This Streamlit app allows vendors to upload their Certificates of Insurance (COIs).
"""

import os
import streamlit as st
from auth import authenticate_user

def main():
    if not authenticate_user():
        st.warning("Please log in to access the upload portal.")
        return

    st.title("Vendor Upload Portal")
    st.write("Upload your Certificate of Insurance (COI) here.")

    uploaded_file = st.file_uploader("Choose a file", type=["pdf", "jpg", "jpeg", "png"])

    if uploaded_file is not None:
        try:
            # Validate file type
            if not uploaded_file.name.endswith(('.pdf', '.jpg', '.jpeg', '.png')):
                st.error("Invalid file type. Please upload a PDF or image file.")
                return

            # Sanitize filename to prevent path traversal
            safe_name = os.path.basename(uploaded_file.name)
            save_path = os.path.join("uploads", safe_name)

            # Save the uploaded file to the uploads directory
            with open(save_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            st.success("File uploaded successfully!")
        except Exception as e:
            st.error(f"An error occurred while uploading the file: {str(e)}")

if __name__ == "__main__":
    main()