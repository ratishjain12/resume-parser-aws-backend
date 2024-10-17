import json
import boto3
import os
from langchain.llms.bedrock import Bedrock
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate

def clean_skills(skills_list):
    # Clean and flatten the skills list
    cleaned_skills = []
    for skill in skills_list:
        # Split skills that are grouped together
        parts = skill.replace('SKILLS:', '').split(',')
        for part in parts:
            # Clean up each skill
            cleaned_skill = part.strip().split(':')[-1].strip()
            if cleaned_skill:
                cleaned_skills.append(cleaned_skill)
    return cleaned_skills

def lambda_handler(event, context):
    try:
        # Get userId from the request body
        body = json.loads(event.get('body', '{}'))
        user_id = body.get('userId')
        
        if not user_id:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'userId is required'}),
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                }
            }

        # Initialize S3 client
        s3 = boto3.client('s3')
        bucket_name = os.environ['BUCKET_NAME']  # Get bucket name from environment variable
        
        # Get the parsed resume data from S3
        try:
            file_key = f'generated/{user_id}_parsed.json'
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            parsed_data = json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            return {
                'statusCode': 404,
                'body': json.dumps({'error': f'Failed to retrieve parsed data: {str(e)}'}),
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                }
            }

        # Extract and clean skills from parsed data
        raw_skills = parsed_data.get('skills', [])
        skills = clean_skills(raw_skills)

        if not skills:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No skills found in parsed data'}),
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Content-Type': 'application/json'
                }
            }

        # Initialize AWS Bedrock client
        bedrock_client = boto3.client(
            service_name='bedrock-runtime',
            region_name='ap-south-1'
        )
        
        llm = Bedrock(
            client=bedrock_client,
            model_id="mistral.mixtral-8x7b-instruct-v0:1"
        )

        # Create a more detailed prompt that includes relevant experience and project info
        prompt_template = """
        Given a candidate with the following background:
        
        Skills: {skills}
        Projects: {projects}
        
        Generate 10 technical interview questions that:
        1. Focus primarily on assessing the listed skills
        2. Include practical scenarios based on their project experience
        3. Test both theoretical knowledge and practical application
        4. Are challenging but appropriate for their experience level
        
        Provide only the questions as a numbered list, without any additional text.
        """

        # Format the skills list and get projects info
        skills_str = ", ".join(skills)
        projects_str = parsed_data.get('projects', '')

        # Create the prompt using the formatted information
        prompt = PromptTemplate(
            input_variables=["skills", "projects"],
            template=prompt_template
        )
        
        chain = LLMChain(llm=llm, prompt=prompt)
        result = chain.run({
            "skills": skills_str,
            "projects": projects_str
        })

        # Split the result into separate questions and clean them
        questions = [
            q.strip().lstrip('0123456789.)-● ') 
            for q in result.strip().split('\n')
            if q.strip()
        ]

        # Return the generated questions as JSON
        return {
            'statusCode': 200,
            'body': json.dumps({
                'questions': questions,
                'skills_used': skills  # Include the extracted skills for reference
            }),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            }
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'}),
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            }
        }