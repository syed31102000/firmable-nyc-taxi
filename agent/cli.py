from agent.sql_agent import answer_question

def main():
    print("NYC Taxi Analytics Agent (type 'exit' to quit)\n")

    while True:
        question = input("You: ").strip()
        if question.lower() in {"exit", "quit"}:
            break

        try:
            sql, df, summary = answer_question(question)
            print("\nGenerated SQL:\n")
            print(sql)
            print("\nSummary:\n")
            print(summary)
            print("\nResult preview:\n")
            print(df.head(20).to_string(index=False))
            print("\n" + "-" * 80 + "\n")
        except Exception as e:
            print(f"\nError: {e}\n")

if __name__ == "__main__":
    main()