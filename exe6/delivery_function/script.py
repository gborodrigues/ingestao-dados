# Descomente a linha debaixo para testes locais, usar a função handler pra input da lambda
# if __name__ == "__main__":
def handler(event, _):
    try:
        print('Finished script')
    except KeyboardInterrupt:
        print("Interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")