from langchain_groq import ChatGroq
import os

os.environ["GROQ_API_KEY"] = "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" 

model=ChatGroq(model="llama-3.3-70b-versatile")

def add(a,b):
    return a+b

response=model.invoke("what is transformer")
print(response)