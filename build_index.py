import os
from pypdf import PdfReader
from sentence_transformers import SentenceTransformer
import chromadb

# Read all PDFs and text files from docs/
documents = []
docs_dir = "docs"

for filename in os.listdir(docs_dir):
    filepath = os.path.join(docs_dir, filename)
    
    if filename.endswith('.pdf'):
        try:
            reader = PdfReader(filepath)
            text = ""
            for page in reader.pages:
                text += page.extract_text() + "\n"
            documents.append(text)
            print(f"✓ Loaded PDF: {filename}")
        except Exception as e:
            print(f"✗ Error reading {filename}: {e}")
    
    elif filename.endswith('.txt'):
        with open(filepath, 'r', encoding='utf-8') as f:
            documents.append(f.read())
            print(f"✓ Loaded TXT: {filename}")

if not documents:
    print("⚠ No documents found in docs/ folder")
    exit(1)

# Split into chunks
chunks = []
for doc in documents:
    paragraphs = [p.strip() for p in doc.split('\n\n') if p.strip()]
    chunks.extend(paragraphs)

print(f"\n📚 Processing {len(chunks)} text chunks...")

# Create embeddings
model = SentenceTransformer('all-MiniLM-L6-v2')
embeddings = model.encode(chunks)

# Store in ChromaDB
client = chromadb.PersistentClient(path="./chroma_db")

# Delete existing collection if it exists
try:
    client.delete_collection("y14m_docs")
except:
    pass

collection = client.create_collection("y14m_docs")
collection.add(
    documents=chunks,
    embeddings=embeddings.tolist(),
    ids=[f"chunk_{i}" for i in range(len(chunks))]
)

print(f"✅ Index built successfully! {len(chunks)} chunks stored.")