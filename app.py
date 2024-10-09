import streamlit as st
import openai


client = openai.OpenAI(base_url="https://llama.us.gaianet.network/v1", api_key="GAIA")



st.set_page_config(
    page_title="Llama Chat",
    page_icon="💬",
    layout="centered"
)


if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


st.title("Llama ChatBot")


for message in st.session_state.chat_history:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])



user_prompt = st.chat_input("Ask llama...")

if user_prompt:
   
    st.chat_message("user").markdown(user_prompt)
    st.session_state.chat_history.append({"role": "user", "content": user_prompt})

  
    response = client.chat.completions.create(
        model="llama",
        messages=[
            {"role": "system", "content": "You are a helpful assistant"},
            *st.session_state.chat_history
        ]
    )

    assistant_response = response.choices[0].message.content
    st.session_state.chat_history.append({"role": "assistant", "content": assistant_response})

    
    with st.chat_message("assistant"):
        st.markdown(assistant_response)
