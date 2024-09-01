import json
import boto3
from langchain.llms.bedrock import Bedrock
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate

def lambda_handler(event, context):
    # Extract skills from the incoming event (JSON data)
    skills = event.get('skills', [])

    if not skills:
        return {
            'statusCode': 400,
            'body': json.dumps('No skills provided')
        }

    # Initialize AWS Bedrock client
    bedrock_client = boto3.client(service_name='bedrock-runtime', region_name='ap-south-1')  # Use boto3 to connect to AWS Bedrock
    llm = Bedrock(client=bedrock_client, model_id="meta.llama3-8b-instruct-v1:0")  # Replace "your-model-id" with the appropriate model ID

    # Define the prompt template for generating questions
    prompt_template = """
    For the skill '{skill}', generate 3 challenging interview questions.
    """

    # Prepare a dictionary to store the generated questions
    generated_questions = {}

    # Generate questions for each skill
    for skill in skills:
        prompt = PromptTemplate(input_variables=["skill"], template=prompt_template)
        chain = LLMChain(llm=llm, prompt=prompt)
        result = chain.run({"skill": skill})

        # Split the result into separate questions
        questions = result.strip().split("\n")
        generated_questions[skill] = questions

    # Return the generated questions as JSON
    return {
        'statusCode': 200,
        'body': json.dumps(generated_questions),
        'headers': {
                'Access-Control-Allow-Origin': '*',
            }
    }