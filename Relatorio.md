# Relatório - Exercício Programático 3

---

## Brian Andreossi - 11060215 <br>Lucas Theodoro - 11072416

---

## 1. Introdução
Este relatório pretende elucidar o sistema desenvolvido como solução para o problema do **Exercício Programático Três**. Para efeito de entendimento mais claro sobre o sistema, foi utilizado a biblioteca Gson (disponível em [github.com/google/gson](https://github.com/google/gson)) para tratamento de objetos que foram enviados pela rede, a biblioteca Jsoup para download de páginas web (disponível em [jsoup.org/](https://jsoup.org/)). Além disso, foi utilizado o maven para gerenciamento de dependências.

---
## 2. Requisitos
A seguir, demonstra-se os requisitos do projeto e se o desenvolvedor os alcançou.

O cliente deveria:
- [x] Enviar mensagem contendo urls de sites que serão lidos de uma pasta local;
- [x] Receber o conteúdo processado pelo resto do sistema e o armazena em um arquivo na mesma pasta local

O servidor coordenador deveria:
- [x] Receber a lista enviada pelo cliente;
- [x] Dividir em listas menores, enviando cada uma para um mapper;

Os mappers deveriam:
- [x] Receber sua parte da lista original, dividida pelo coordenador;
- [x] Realizar a atividade atribuída a ele;
- [x] Enviar o resultado ao Reducer;

O reducer deveria:
- [x] Ao receber todos os resultados dos mappers, gerar o índice invertido;
- [x] Enviar ao cliente

---
## 3. Mensagem
### 3.1 Atributos da mensagem
A seguir, uma tabela dos atributos da classe mensagem, seguidas de uma breve explicação.

|  Atributo  | Tipo   |  Explicação |
|:----------:|--------|:------------|
| id         | **Long**\*  | Guarda o id da mensagem. (único em relação a conversa) |
| end        | **Long**\* | Armazena a quantidade de mensagens que a lista do cliente foi quebrada |
| content    | Object | O conteúdo referente a mensagem |
| to         | **User**\* |  Usuário que receberá a mensagem (para este contexto, será sempre o servidor) |
| from       | **User**\* |  Usuário que enviou a mensagem |
| requestor | **User**\*  | Usuário que fez a requisição (Cliente) |

\* - Classe Long, definido na biblioteca principal do java.  
\* - User, classe definida neste projeto.

É importante destacar que todas as mensagens entre os elementos deste projeto seguem este padrão. O que difere cada tipo de mensagem é o conteúdo (que pode ser qualquer objeto) e o usuário que a enviou.

Atributos como o **end**, **requestor** e **id** (assim como os outros), não são obrigatórios, podendo ser usado o mesmo objeto para qualquer comunicação que seja necessária entre quaisquer dois elementos.

---
## 4. Funcionamento do Reducer
O Reducer guarda cada mensagem que recebe em um mapa, que liga o solicitante (cliente) à lista de mensagens que o mapper enviou.

Cada uma das mensagens possuem um **id** que mostra a ordem dela em relação à lista original (que neste escopo não teve necessidade) e um **end** que denota a quantidade de mensagens que a lista foi quebrada.

No momento que a lista que está relacionada ao requisitante possui a exata quantidade antes da divisão da lista, então é feito a criação do índice invertido, enviado ao cliente a resposta e retirado do mapa a tupla (solicitante, **array** de listas).
