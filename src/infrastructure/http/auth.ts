// ====================================================================================================
//                  AUTENTICAÇÃO: PONTO DE EXTENSÃO EXPLÍCITO (SEÇÃO 2 DO DESAFIO)
// ====================================================================================================
// Sem Identity Provider integrado, o adaptador da porta é um no-op que não restringe nada. O desenho
// com Keycloak (client credentials, JWT validado com jose pelo JWKS) está no ARCHITECTURE.md; para
// ligá-lo basta trocar o adaptador aqui, e o guard global já chama a porta em toda rota não pública.
import { Injectable, SetMetadata, type CanActivate, type ExecutionContext } from "@nestjs/common";
import { Reflector } from "@nestjs/core";                                                       // lê a marca de rota pública
import type { Request } from "express";

export interface ProviderIdentity {
  providerId: string | undefined;                                                               // undefined: sem IdP, ninguém é identificado
}

export interface ProviderIdentityPort {
  identify(request: Request): Promise<ProviderIdentity>;                                        // com IdP: valida o token ou lança 401
}

export class NoopProviderIdentity implements ProviderIdentityPort {
  async identify(): Promise<ProviderIdentity> {
    return { providerId: undefined };
  }
}

export const IS_PUBLIC_ROUTE = "isPublicRoute";
export const PublicRoute = () => SetMetadata(IS_PUBLIC_ROUTE, true);                            // health e /metrics

@Injectable()
export class AuthGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    private readonly identityPort: ProviderIdentityPort,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isPublic = this.reflector.getAllAndOverride<boolean>(IS_PUBLIC_ROUTE, [context.getHandler(), context.getClass()]);
    if (isPublic) return true;
    const request = context.switchToHttp().getRequest<Request & { providerIdentity?: ProviderIdentity }>();
    request.providerIdentity = await this.identityPort.identify(request);                       // com IdP: comparar com o providerId do corpo (403 PROVIDER_MISMATCH)
    return true;
  }
}
