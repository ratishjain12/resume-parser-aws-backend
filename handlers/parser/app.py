import re
import json
import nltk
import boto3
from nltk import word_tokenize, pos_tag, ne_chunk
from nltk.tree import Tree
from pdfminer.high_level import extract_text

# Specify the path to the NLTK data in the Lambda layer
nltk.data.path.append("/opt/nltk_data")

s3 = boto3.client('s3')

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    return extract_text(pdf_path)

def extract_contact_info(text):
    """Extract email and phone numbers from text."""
    email = re.findall(r'\S+@\S+', text)
    phone = re.findall(r'\+?\d[\d -]{8,}\d', text)  # Regex for phone numbers
    return {"email": email, "phone": phone}

def extract_name(text):
    """Extract the name by using NLTK's named entity recognition."""
    lines = text.strip().splitlines()
    for line in lines[:10]:  # Consider the first few lines for the name
        tokenized_line = word_tokenize(line)
        tagged_line = pos_tag(tokenized_line)
        named_entities = ne_chunk(tagged_line)
        
        for chunk in named_entities:
            if isinstance(chunk, Tree) and chunk.label() == 'PERSON':
                return ' '.join([c[0] for c in chunk])
    
    return None

def extract_skills(text):
    """Extract skills from the 'Technical Skills' section of the resume."""
    skills_section_pattern = re.compile(
        r'(TECHNICAL SKILLS|SKILLS)(.*?)(?=PROJECTS|CERTIFICATIONS|EDUCATION|CERTIFICATES|ACHIEVEMENTS|$)', 
        re.DOTALL | re.IGNORECASE
    )
    skills_section = skills_section_pattern.search(text)
    
    if skills_section:
        skills_text = skills_section.group(2)
        skills_text = remove_bullets(skills_text)
        skills = re.split(r'[,\n•;]', skills_text)
        skills = [
            skill.strip() for skill in skills 
            if skill.strip() and not re.search(r'\b(?:\d{4}|\d{1,2}/\d{1,2}/\d{2,4})\b', skill.strip())
        ]
        return skills
    
    return None

def extract_experience(text):
    """Extract experience sections based on headers in uppercase."""
    experience_pattern = re.compile(
        r'(EXPERIENCE|EMPLOYMENT HISTORY|WORK HISTORY|PROFESSIONAL EXPERIENCE|CAREER SUMMARY)\s*[\n\r]+(.*?)(?=\n[A-Z\s]+\n|\Z)',
        re.DOTALL | re.IGNORECASE
    )
    matches = experience_pattern.findall(text)
    
    if matches:
        experience_texts = []
        for match in matches:
            header, content = match
            content = content.strip()
            if content:
                experience_texts.append(remove_bullets(content))

        return "\n\n".join(experience_texts) if experience_texts else None

    return None

def extract_education(text):
    """Extract education details."""
    education_pattern = re.compile(r'(B\.Sc|B\.Eng|M\.Sc|M\.Eng|Bachelor|Master|Ph\.D|Doctorate|Diploma).*?(Computer Science|Engineering|Data Science|Information Technology|Business|Mathematics)', re.DOTALL | re.IGNORECASE)
    education_text = education_pattern.findall(text)
    return [remove_bullets(item) for sublist in education_text for item in sublist]

def extract_projects(text):
    """Extract the 'Relevant Projects' section with full content until the next major header."""
    projects_pattern = re.compile(
        r'(RELEVANT PROJECTS|PROJECTS)(.*?)(?=(EDUCATION|SKILLS|ACHIEVEMENTS & CERTIFICATIONS|CERTIFICATIONS|$|\n[A-Z]+\n|\Z))', 
        re.DOTALL | re.IGNORECASE
    )
    match = projects_pattern.search(text)
    
    if match:
        projects_text = match.group(2).strip()
        return projects_text

    return None

def extract_certifications(text):
    """Extract all lines from the certifications section."""
    certifications_pattern = re.compile(
        r'(certifications?|courses?|accreditations?|certificates?|achievements?)'
        r'([\s\S]*?)(certificates|achievements|$)',
        re.IGNORECASE
    )
    
    matches = certifications_pattern.findall(text)
    
    if matches:
        certifications_content = matches[0][1].strip()
        certifications_content = remove_bullets(certifications_content)
        certifications_lines = [line.strip() for line in certifications_content.splitlines() if line.strip()]
        return "\n".join(certifications_lines)

    return None

def remove_bullets(text):
    """Remove common bullet points and symbols from text."""
    text = re.sub(r'^\s*[-•*]\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r'\n\s*[-•*]\s+', '\n', text)
    return text

def parse_resume(pdf_path):
    """Main function to parse resume and extract structured information."""
    text = extract_text_from_pdf(pdf_path)
    
    name = extract_name(text)
    contact_info = extract_contact_info(text)
    skills = extract_skills(text)
    experience = extract_experience(text)
    education = extract_education(text)
    projects = extract_projects(text)
    certifications = extract_certifications(text)
    
    return {
        "name": name,
        "contact_info": contact_info,
        "skills": skills,
        "experience": experience,
        "education": education,
        "projects": projects,
        "certifications": certifications
    }

def lambda_handler(event, context):
    """AWS Lambda function entry point."""
    # Extract the bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Download the file from S3 to the /tmp directory in Lambda
    pdf_path = "/tmp/resume.pdf"
    s3.download_file(bucket_name, object_key, pdf_path)
    
    # Parse the resume
    parsed_data = parse_resume(pdf_path)
    
    # Return the parsed data as a JSON response
    return {
        'statusCode': 200,
        'body': json.dumps(parsed_data)
    }
