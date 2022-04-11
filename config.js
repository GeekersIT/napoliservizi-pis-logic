const config = {
    keycloak: {
        url: "https://" + process.env.PUBLIC_URI + "/auth",
        username: process.env.ADMIN_USER,
        password: process.env.ADMIN_PWD,
        realm: process.env.NAMESPACE,
        secret: process.env.KEYCLOAK_ADMIN_SECRET
    },
    db: {
        url: "http://hasura."+process.env.NAMESPACE+"/v1/graphql",
    },
    minio: {
        url: "minio."+process.env.NAMESPACE,
        accessKey: process.env.MINIO_ACCESS,
        secretKey: process.env.MINIO_SECRET
    },
    template: {
        url: "http://minio."+process.env.NAMESPACE + ":9000/template/"
    },
    protocollo: {
        url: "http://protocollo."+process.env.NAMESPACE
    }
};

export default config;