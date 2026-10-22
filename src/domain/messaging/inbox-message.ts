// ====================================================================================================
//                         INBOX: MENSAGEM DA FILA JÁ VISTA POR UM CONSUMIDOR
// ====================================================================================================
// Gravada no mesmo commit do efeito, então "existe na inbox" quer dizer "já foi processada".

export interface ReceiveInboxProps {
  messageId: string;
  consumerName: string;
  payloadHash: string;                                                                          // o mesmo payloadHash da operação
  receivedAt: Date;
}

export interface InboxMessageState extends ReceiveInboxProps {
  processedAt: Date | undefined;
}

export class InboxMessage {
  private constructor(
    public readonly messageId: string,
    public readonly consumerName: string,
    public readonly payloadHash: string,
    public readonly receivedAt: Date,
    private _processedAt?: Date,
  ) {}

  static receive(props: ReceiveInboxProps): InboxMessage {
    return new InboxMessage(props.messageId, props.consumerName, props.payloadHash, props.receivedAt);
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(state.messageId, state.consumerName, state.payloadHash, state.receivedAt, state.processedAt);
  }

  get processedAt(): Date | undefined {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== undefined;
  }

  markProcessed(at: Date): void {
    this._processedAt = at;
  }

  isSameMessage(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;                                                    // mesmo id e hash diferente é produtor reutilizando id
  }
}
