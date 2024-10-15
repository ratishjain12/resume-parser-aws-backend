import re
import json
import boto3
from pdfminer.high_level import extract_text_to_fp
from io import BytesIO
import logging
import urllib.parse  

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

def extract_text_from_pdf(pdf_stream):
    """Extract text from a PDF file byte stream."""
    try:
        output_string = BytesIO()
        extract_text_to_fp(pdf_stream, output_string)
        text = output_string.getvalue().decode('utf-8')
        logger.info("PDF text extracted successfully")
        return text
    except Exception as e:
        logger.error(f"Error extracting text from PDF: {str(e)}")
        raise

def extract_contact_info(text):
    """Extract email and phone numbers from text."""
    try:
        email = re.findall(r'\b\S+@\S+\.\S+\b', text)
        phone = re.findall(r'\+?\d[\d\s\-()]{8,}\d', text)  # Enhanced Regex for phone numbers
        logger.info(f"Extracted Contact Info - Email: {email}, Phone: {phone}")
        return {"email": email, "phone": phone}
    except Exception as e:
        logger.error(f"Error extracting contact info: {str(e)}")
        raise


def extract_skills(text):
    """Extract skills from the 'Technical Skills' section of the resume."""
    try:
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
            logger.info(f"Extracted Skills: {skills}")
            return skills
        
        logger.info("Skills section not found")
        return None
    except Exception as e:
        logger.error(f"Error extracting skills: {str(e)}")
        raise

def extract_experience(text):
    """Extract experience sections based on headers in uppercase."""
    try:
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

            experience = "\n\n".join(experience_texts) if experience_texts else None
            logger.info(f"Extracted Experience: {experience[:200]}...")  # Log first 200 characters
            return experience

        logger.info("Experience section not found")
        return None
    except Exception as e:
        logger.error(f"Error extracting experience: {str(e)}")
        raise

def extract_education(text):
    """Extract education details."""
    try:
        education_pattern = re.compile(r'(B\.Sc|B\.Eng|M\.Sc|M\.Eng|Bachelor|Master|Ph\.D|Doctorate|Diploma).*?(Computer Science|Engineering|Data Science|Information Technology|Business|Mathematics)', re.DOTALL | re.IGNORECASE)
        education_text = education_pattern.findall(text)
        education = [remove_bullets(item) for sublist in education_text for item in sublist]
        logger.info(f"Extracted Education: {education}")
        return education
    except Exception as e:
        logger.error(f"Error extracting education: {str(e)}")
        raise

def extract_projects(text):
    """Extract the 'Relevant Projects' section with full content until the next major header."""
    try:
        projects_pattern = re.compile(
            r'(RELEVANT PROJECTS|PROJECTS)(.*?)(?=(EDUCATION|SKILLS|ACHIEVEMENTS & CERTIFICATIONS|CERTIFICATIONS|$|\n[A-Z]+\n|\Z))', 
            re.DOTALL | re.IGNORECASE
        )
        match = projects_pattern.search(text)
        
        if match:
            projects_text = match.group(2).strip()
            logger.info(f"Extracted Projects: {projects_text[:200]}...")  # Log first 200 characters
            return projects_text

        logger.info("Projects section not found")
        return None
    except Exception as e:
        logger.error(f"Error extracting projects: {str(e)}")
        raise

def extract_certifications(text):
    """Extract all lines from the certifications section."""
    try:
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
            certifications = "\n".join(certifications_lines)
            logger.info(f"Extracted Certifications: {certifications}")
            return certifications

        logger.info("Certifications section not found")
        return None
    except Exception as e:
        logger.error(f"Error extracting certifications: {str(e)}")
        raise

def remove_bullets(text):
    """Remove common bullet points and symbols from text."""
    try:
        text = re.sub(r'^\s*[-•*]\s+', '', text, flags=re.MULTILINE)
        text = re.sub(r'\n\s*[-•*]\s+', '\n', text)
        return text
    except Exception as e:
        logger.error(f"Error removing bullets: {str(e)}")
        raise

def parse_resume(pdf_stream):
    """Main function to parse resume and extract structured information."""
    try:
        text = extract_text_from_pdf(pdf_stream)
        logger.info(f"Extracted Text: {text[:200]}...")  # Log the first 200 characters of the text

        contact_info = extract_contact_info(text)
        skills = extract_skills(text)
        experience = extract_experience(text)
        education = extract_education(text)
        projects = extract_projects(text)
        certifications = extract_certifications(text)

        parsed_data = {
            "contact_info": contact_info,
            "skills": skills,
            "experience": experience,
            "education": education,
            "projects": projects,
            "certifications": certifications
        }
        logger.info(f"Parsed Data: {json.dumps(parsed_data, indent=2)}")
        return parsed_data
    except Exception as e:
        logger.error(f"Error in parse_resume: {str(e)}", exc_info=True)
        raise

def lambda_handler(event, context):
    """AWS Lambda function entry point."""
    try:
        logger.info(f"Received event: {json.dumps(event)}")
        
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        encoded_object_key = event['Records'][0]['s3']['object']['key']

        # Decode the object key to handle spaces and special characters
        object_key = urllib.parse.unquote_plus(encoded_object_key)
        logger.info(f"Bucket: {bucket_name}, Decoded Object Key: {object_key}")

        # Extract userid from the object key
        temp_split = object_key.split('/')[1]
        userid = temp_split.split('_')[0]
        logger.info(f"Extracted User ID: {userid}")

        # Retrieve the PDF from S3
        s3_response = s3.get_object(Bucket=bucket_name, Key=object_key)
        pdf_stream = BytesIO(s3_response['Body'].read())

        logger.info("Successfully retrieved object from S3")

        # Parse the resume
        parsed_data = parse_resume(pdf_stream)

        # Store parsed data as JSON in the same S3 bucket
        parsed_json = json.dumps(parsed_data)
        parsed_key = f"generated/{userid}_parsed.json"
        logger.info(f"Attempting to store parsed data in S3: {parsed_key}")
        s3.put_object(Bucket=bucket_name, Key=parsed_key, Body=parsed_json)
        logger.info(f"Stored parsed data in S3: {parsed_key}")

        return {
            'statusCode': 200,
            'body': json.dumps(parsed_data)
        }
    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }