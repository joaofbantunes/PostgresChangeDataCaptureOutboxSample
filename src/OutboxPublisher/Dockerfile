FROM mcr.microsoft.com/dotnet/aspnet:6.0-alpine AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/sdk:6.0-alpine AS build
WORKDIR /src
COPY ["src/OutboxPublisher/OutboxPublisher.csproj", "OutboxPublisher/"]
COPY ["src/Events/Events.csproj", "Events/"]
RUN dotnet restore "OutboxPublisher/OutboxPublisher.csproj"
COPY ["src/OutboxPublisher/.", "OutboxPublisher/"]
COPY ["src/Events/.", "Events/"]
WORKDIR "/src/OutboxPublisher"

FROM build AS publish
RUN dotnet publish "OutboxPublisher.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OutboxPublisher.dll"]
