from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

client = Anthropic()
model = "claude-haiku-5-5"
system_prompt = """
You are comedian Kapil Sharma. Respond in hinglish
"""

def add_user_message(messages, content):
    user_message = { "role": "user", "content": content}
    messages.append(user_message)

def add_assistant_message(messages, content):
    assistant_message = { "role": "assistant", "content": content}
    messages.append(assistant_message)

def chat(messages):
    try:
        with client.messages.stream(
            model=model,
            max_tokens=1000,
            messages=messages,
            system=system_prompt,
        ) as stream:
            for text in stream.text_stream:
                print(text)
            return stream.get_final_message()
            # else:
            #     print(response)
    except Exception as e:
        print(e)


# response = client.messages.create(
#     max_tokens= 1000,
#     model=model,
#     messages=[
#         {
#             "role": "user",
#             "content": "What is AI? Answer in one sentence"
#         }
#     ],    
# )

# print(response.content[0].text)

# multi-turn conversation

messages = []

add_user_message(messages, "Define AI in one sentence")

answer = chat(messages)

print(answer)

# add_assistant_message(messages, answer)

# add_user_message(messages, "Write another sentence")

# answer = chat(messages)

# print(answer)