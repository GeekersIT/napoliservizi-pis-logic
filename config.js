const config = {
    keycloak: {
        url: "https://" + process.env.PUBLIC_URI + "/auth",
        username: process.env.ADMIN_USER,
        password: process.env.ADMIN_PWD,
        realm: process.env.KEYCLOAK_REALM,
        secret: process.env.KEYCLOAK_ADMIN_SECRET
    },
    db: {
        url: "hasura."+process.env.NAMESPACE+"/db/v1/graphql",
    },
    minio: {
        url: "minio."+process.env.NAMESPACE,
        accessKey: process.env.MINIO_ACCESS,
        secretKey: process.env.MINIO_SECRET
    },
    template: {
        url: "minio."+process.env.NAMESPACE + "/template/"
    },
    protocollo: {
        url: "protocollo-service"+process.env.NAMESPACE + ":8080"
    }
};

export default config;